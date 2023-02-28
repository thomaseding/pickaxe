#ifndef THE_PICKAXE_HPP
#define THE_PICKAXE_HPP

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <string>
#include <type_traits>
#include <vector>

namespace pickaxe {

  class Exception : public std::exception {
    std::string _message;
  
  public:
    Exception(
      std::string const &message)
      : _message(message)
    {}

    char const *what() const noexcept override
    {
      return _message.c_str();
    }
  };

  class CloseException : public Exception {
  public:
    CloseException(
      std::string const &filename)
      : Exception("failed to close '" + filename + "'")
    {}
  };

  class WriteException : public Exception {
  public:
    WriteException(
      std::string const &filename)
      : Exception("failed to write '" + filename + "'")
    {}

    WriteException(
      std::string const &filename,
      std::string const &message)
      : Exception("failed to write '" + filename + "': " + message)
    {}
  };

  class ReadException : public Exception {
  public:
    ReadException(
      std::string const &filename)
      : Exception("failed to read '" + filename + "'")
    {}

    ReadException(
      std::string const &filename,
      std::string const &message)
      : Exception("failed to read '" + filename + "': " + message)
    {}
  };

  class InvalidPageSizeException : public Exception {
  public:
    InvalidPageSizeException(
      uint64_t page_size)
      : Exception("invalid page size: " + std::to_string(page_size))
    {}
  };

  // Since destructors shouldn't throw, this is used to store any exceptions
  // the destructor would throw. It is the user's responsibility to make sure
  // this outlives the Serializer or Deserializer associated with it. It is the
  // user's responsibility to check this for any set exceptions.
  class DestructorExceptions {
  public:
    std::vector<CloseException> close;

    [[nodiscard]] bool is_empty() const
    {
      return close.empty();
    }

    void clear()
    {
      close.clear();
    }
  };

  class Serializer {
    static constexpr size_t _num_zeroes = alignof(std::max_align_t);
    static constinit std::byte const _zeroes[_num_zeroes] = {0};

    FILE *_file;
    uint64_t _offset;
    DestructorExceptions *_exceptions;
    std::string _filename;

  public:
    ~Serializer() noexcept
    {
      try {
        _close();
      }
      catch (CloseException const &e) {
        _exceptions->exceptions.push_back(e);
      }
    }

    Serializer(
      DestructorExceptions &exceptions,
      char const *filename)
      : _file(std::fopen(filename, "wb"))
      , _offset(0)
      , _exceptions(&exceptions)
      , _filename(filename)
    {
      if (_file == nullptr) {
        throw WriteException(_filename, "failed to open");
      }
    }

    Serializer(
      Serializer &&other) noexcept
      : _file(std::move(other._file))
      , _offset(std::move(other._offset))
      , _exceptions(std::move(other._exceptions))
      , _filename(std::move(other._filename))
    {
      other._file = nullptr;
    }

    Serializer(Serializer const &) = delete;

    Serializer &operator=(
      Serializer &&other) noexcept
    {
      _close();
      _file = std::move(other._file);
      _offset = std::move(other._offset);
      _exceptions = std::move(other._exceptions);
      _filename = std::move(other._filename);
      other._file = nullptr;
      return *this;
    }

    Serializer &operator=(Serializer const &) = delete;

    [[nodiscard]] uint64_t get_offset() const
    {
      return _offset;
    }

    void set_offset(
      uint64_t new_offset)
    {
      auto ret = std::fseek(_file, new_offset, SEEK_SET);
      if (ret != 0) {
        throw WriteException(_filename, "failed to seek");
      }
      _offset = new_offset;
    }

    void set_offset_aligned(
      uint64_t new_offset,
      uint64_t alignment)
    {
      uint64_t mod = new_offset % alignment;
      if (mod != 0) {
        new_offset += alignment - mod;
      }
      set_offset(new_offset);
    }

    template <typename T>
    void write(
      T const &data)
    {
      static_assert(std::is_pod_v<T>);
      write(reinterpret_cast<std::byte const *>(&data), sizeof(T));
    }

    template <typename T>
    void write_aligned(
      T const &data)
    {
      static_assert(std::is_pod_v<T>);
      write_aligned(reinterpret_cast<std::byte const *>(&data), sizeof(T), alignof(T));
    }

    void write(
      void const *data,
      uint64_t size)
    {
      auto n = std::fwrite(data, 1, size, _file);
      if (n != size) {
        throw WriteException(_filename);
      }
      _offset += size;
    }

    void write_aligned(
      void const *data,
      uint64_t size,
      uint64_t alignment)
    {
      uint64_t mod = _offset % alignment;
      if (mod != 0) {
        uint64_t padding = alignment - mod;
        while (padding > _num_zeroes) {
          write(_zeroes, _num_zeroes);
          padding -= _num_zeroes;
        }
        write(_zeroes, padding);
      }
      write(data, size);
    }

    void flush()
    {
      auto ret = std::fflush(_file);
      if (ret != 0) {
        throw WriteException(_filename, "failed to flush");
      }
    }
    
  private:
    void _close()
    {
      if (_file != nullptr) {
        auto ret = std::fclose(_file);
        if (ret != 0) {
          throw CloseException(_filename);
        }
        _file = nullptr;
      }
    }
  };

  class Deserializer {
    FILE *_file;
    uint64_t _target_page_size;
    uint64_t _active_page_size;
    uint64_t _file_offset_page_begin;
    uint64_t _file_offset_page_end;
    uint64_t _read_buffer_offset;
    DestructorExceptions *_exceptions;
    std::string _filename;
    std::vector<std::byte> _read_buffer;

  public:
    ~Deserializer() noexcept
    {
      try {
        _close();
      }
      catch (CloseException const &e) {
        _exceptions->exceptions.push_back(e);
      }
    }

    Deserializer(
      DestructorExceptions &exceptions,
      char const *filename,
      uint64_t page_size)
      : _file(std::fopen(filename, "rb"))
      , _target_page_size(page_size)
      , _active_page_size(0)
      , _file_offset_page_begin(0)
      , _file_offset_page_end(0)
      , _read_buffer_offset(page_size)
      , _exceptions(&exceptions)
      , _filename(filename)
      , _read_buffer(std::byte{}, page_size)
    {
      if (_file == nullptr) {
        throw ReadException(_filename, "failed to open");
      }
      if (page_size == 0) {
        throw InvalidPageSizeException(page_size);
      }
    }

    Deserializer(
      Deserializer &&other) noexcept
      : _file(std::move(other._file))
      , _target_page_size(std::move(other._target_page_size))
      , _active_page_size(std::move(other._active_page_size))
      , _file_offset_page_begin(std::move(other._file_offset_page_begin))
      , _file_offset_page_end(std::move(other._file_offset_page_end))
      , _read_buffer_offset(std::move(other._read_buffer_offset))
      , _exceptions(std::move(other._exceptions))
      , _filename(std::move(other._filename))
      , _read_buffer(std::move(other._read_buffer))
    {
      other._file = nullptr;
    }

    Deserializer(Deserializer const &) = delete;

    Deserializer &operator=(
      Deserializer &&other) noexcept
    {
      _close();
      _file = std::move(other._file);
      _target_page_size = std::move(other._target_page_size);
      _active_page_size = std::move(other._active_page_size);
      _file_offset_page_begin = std::move(other._file_offset_page_begin);
      _file_offset_page_end = std::move(other._file_offset_page_end);
      _read_buffer_offset = std::move(other._read_buffer_offset);
      _exceptions = std::move(other._exceptions);
      _filename = std::move(other._filename);
      _read_buffer = std::move(other._read_buffer);
      other._file = nullptr;
      return *this;
    }

    Deserializer &operator=(Deserializer const &) = delete;

    [[nodiscard]] uint64_t get_page_size() const
    {
      return _target_page_size;
    }

    void set_page_size(
      uint64_t new_page_size)
    {
      if (new_page_size == 0) {
        throw InvalidPageSizeException(new_page_size);
      }
      _target_page_size = new_page_size;
      if (_read_buffer.size() < _target_page_size) {
        _read_buffer.resize(_target_page_size);
      }
    }

    [[nodiscard]] uint64_t get_offset() const
    {
      return _file_offset_page_begin + _read_buffer_offset;
    }

    void set_offset(
      uint64_t new_offset)
    {
      if (_file_offset_page_begin <= new_offset && new_offset < _file_offset_page_begin + _active_page_size) {
        auto delta = new_offset - _file_offset_page_begin;
        _read_buffer_offset = delta;
        return;
      }
      auto ret = std::fseek(_file, new_offset, SEEK_SET);
      if (ret != 0) {
        throw ReadException(_filename);
      }
      _file_offset_page_begin = new_offset;
      _file_offset_page_end = new_offset;
      _read_buffer_offset = _active_page_size;
    }

    [[nodiscard]] bool is_eof() const
    {
      return std::feof(_file);
    }

    template <typename T>
    void read(
      T &dest)
    {
      static_assert(std::is_pod_v<T>);
      read(reinterpret_cast<std::byte *>(&dest), sizeof(T));
    }

    template <typename T>
    void read_aligned(
      T &dest)
    {
      static_assert(std::is_pod_v<T>);
      read_aligned(reinterpret_cast<std::byte *>(&dest), sizeof(T), alignof(T));
    }

    void read(
      std::byte *dest,
      uint64_t size)
    {
      while (_read_buffer_offset + size > _active_page_size) {
        uint64_t lead = _active_page_size - _read_buffer_offset;
        std::memcpy(dest, _read_buffer.data() + _read_buffer_offset, lead);
        dest += lead;
        size -= lead;
        auto n = _read_page();
        if (n < _active_page_size && n < size) {
          throw ReadException(_filename, "not enough remaining bytes at current offset");
        }
      }
      std::memcpy(dest, _read_buffer.data() + _read_buffer_offset, size);
      _read_buffer_offset += size;
    }

    void read_aligned(
      std::byte *dest,
      uint64_t size,
      uint64_t alignment)
    {
      uint64_t mod = _read_buffer_offset % alignment;
      if (mod != 0) {
        uint64_t padding = alignment - mod;
        if (_read_buffer_offset + padding > _active_page_size) {
          (void)_read_page();
        }
        else {
          _read_buffer_offset += padding;
        }
      }
      read(dest, size);
    }

  private:
    [[nodiscard]] uint64_t _read_page()
    {
      if (_active_page_size != _target_page_size) {
        _active_page_size = _target_page_size;
        std::setvbuf(_file, reinterpret_cast<char *>(_read_buffer.data()), _IOFBF, _active_page_size);
      }
      auto size = _active_page_size;
      auto n = std::fread(_read_buffer.data(), 1, size, _file);
      if (n != size) {
        if (std::ferror(_file) || !is_eof()) {
          throw ReadException(_filename);
        }
        size = n;
      }
      _file_offset_page_begin = _file_offset_page_end;
      _file_offset_page_end += size;
      _read_buffer_offset = 0;
      return size;
    }

    void _close()
    {
      if (_file != nullptr) {
        auto ret = std::fclose(_file);
        if (ret != 0) {
          throw CloseException(_filename);
        }
        _file = nullptr;
      }
    }
  };

}

#endif
