# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=uncompressed, compat_level=0

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: large_string
      cat: dictionary<values=large_string, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=zstd, compat_level=0

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: large_string
      cat: dictionary<values=large_string, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=lz4, compat_level=0

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: large_string
      cat: dictionary<values=large_string, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=NULL, compat_level=0

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: large_string
      cat: dictionary<values=large_string, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=uncompressed, compat_level=1

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: string_view
      cat: dictionary<values=string_view, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=zstd, compat_level=1

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: string_view
      cat: dictionary<values=string_view, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=lz4, compat_level=1

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: string_view
      cat: dictionary<values=string_view, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=NULL, compat_level=1

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: string_view
      cat: dictionary<values=string_view, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=uncompressed, compat_level=oldest

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: large_string
      cat: dictionary<values=large_string, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=zstd, compat_level=oldest

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: large_string
      cat: dictionary<values=large_string, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=lz4, compat_level=oldest

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: large_string
      cat: dictionary<values=large_string, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=NULL, compat_level=oldest

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: large_string
      cat: dictionary<values=large_string, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=uncompressed, compat_level=newest

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: string_view
      cat: dictionary<values=string_view, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=zstd, compat_level=newest

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: string_view
      cat: dictionary<values=string_view, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=lz4, compat_level=newest

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: string_view
      cat: dictionary<values=string_view, indices=uint32>

# Test writing data to Arrow file {compression %||% 'NULL'} - {compat_level} compression=NULL, compat_level=newest

    Code
      arrow::read_ipc_file(tmpf, as_data_frame = FALSE, mmap = FALSE)$schema
    Output
      Schema
      int: int32
      chr: string_view
      cat: dictionary<values=string_view, indices=uint32>

