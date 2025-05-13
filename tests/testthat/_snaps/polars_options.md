# options are validated by polars_options() polars.df_knitr_print

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `df_knitr_print` must be one of "auto", not "foo".

# options are validated by polars_options() polars.to_r_vector.uint8

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.uint8` must be one of "integer" or "raw", not "foo".

# options are validated by polars_options() polars.to_r_vector.int64

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.int64` must be one of "double", "character", "integer", or "integer64", not "foo".

# options are validated by polars_options() polars.to_r_vector.date

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.date` must be one of "Date" or "IDate", not "foo".

# options are validated by polars_options() polars.to_r_vector.time

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.time` must be one of "hms" or "ITime", not "foo".

# options are validated by polars_options() polars.to_r_vector.struct

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.struct` must be one of "dataframe" or "tibble", not "foo".

# options are validated by polars_options() polars.to_r_vector.decimal

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.decimal` must be one of "double" or "character", not "foo".

# options are validated by polars_options() polars.to_r_vector.as_clock_class

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.as_clock_class` must be `TRUE` or `FALSE`, not the string "foo".

# options are validated by polars_options() polars.to_r_vector.ambiguous

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.ambiguous` must be one of "raise", "earliest", "latest", or "null", not "foo".

# options are validated by polars_options() polars.to_r_vector.non_existent

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.non_existent` must be one of "raise" or "null", not "foo".

# options for to_r_vector() works: polars.to_r_vector.uint8 = integer

    Code
      series$to_r_vector()
    Output
        uint8 int64       date            time a decimal   duration datetime_without_tz    datetime_with_tz
      1     1     1 1970-01-02 00:00:00.000000 1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00:00:00.000000 2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00:00:00.000000 3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.uint8 = raw

    Code
      series$to_r_vector()
    Message
      `uint8` is overridden by the option "polars.to_r_vector.uint8" with the string "raw"
    Output
        uint8 int64       date            time a decimal   duration datetime_without_tz    datetime_with_tz
      1    01     1 1970-01-02 00:00:00.000000 1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2    02     2 1970-01-03 00:00:00.000000 2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3    03     3 1970-01-04 00:00:00.000000 3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Message
      `uint8` is overridden by the option "polars.to_r_vector.uint8" with the string "raw"
    Output
      $uint8
      [1] 01 02 03
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.int64 = double

    Code
      series$to_r_vector()
    Output
        uint8 int64       date            time a decimal   duration datetime_without_tz    datetime_with_tz
      1     1     1 1970-01-02 00:00:00.000000 1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00:00:00.000000 2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00:00:00.000000 3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.int64 = character

    Code
      series$to_r_vector()
    Message
      `int64` is overridden by the option "polars.to_r_vector.int64" with the string "character"
    Output
        uint8 int64       date            time a decimal   duration datetime_without_tz    datetime_with_tz
      1     1     1 1970-01-02 00:00:00.000000 1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00:00:00.000000 2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00:00:00.000000 3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Message
      `int64` is overridden by the option "polars.to_r_vector.int64" with the string "character"
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] "1" "2" "3"
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.int64 = integer

    Code
      series$to_r_vector()
    Message
      `int64` is overridden by the option "polars.to_r_vector.int64" with the string "integer"
    Output
        uint8 int64       date            time a decimal   duration datetime_without_tz    datetime_with_tz
      1     1     1 1970-01-02 00:00:00.000000 1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00:00:00.000000 2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00:00:00.000000 3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Message
      `int64` is overridden by the option "polars.to_r_vector.int64" with the string "integer"
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.int64 = integer64

    Code
      series$to_r_vector()
    Message
      `int64` is overridden by the option "polars.to_r_vector.int64" with the string "integer64"
    Output
        uint8 int64       date            time a decimal   duration datetime_without_tz    datetime_with_tz
      1     1     1 1970-01-02 00:00:00.000000 1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00:00:00.000000 2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00:00:00.000000 3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Message
      `int64` is overridden by the option "polars.to_r_vector.int64" with the string "integer64"
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      integer64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.date = Date

    Code
      series$to_r_vector()
    Output
        uint8 int64       date            time a decimal   duration datetime_without_tz    datetime_with_tz
      1     1     1 1970-01-02 00:00:00.000000 1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00:00:00.000000 2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00:00:00.000000 3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.date = IDate

    Code
      series$to_r_vector()
    Message
      `date` is overridden by the option "polars.to_r_vector.date" with the string "IDate"
    Output
        uint8 int64       date            time a decimal   duration datetime_without_tz    datetime_with_tz
      1     1     1 1970-01-02 00:00:00.000000 1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00:00:00.000000 2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00:00:00.000000 3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Message
      `date` is overridden by the option "polars.to_r_vector.date" with the string "IDate"
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.time = hms

    Code
      series$to_r_vector()
    Output
        uint8 int64       date            time a decimal   duration datetime_without_tz    datetime_with_tz
      1     1     1 1970-01-02 00:00:00.000000 1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00:00:00.000000 2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00:00:00.000000 3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.time = ITime

    Code
      series$to_r_vector()
    Message
      `time` is overridden by the option "polars.to_r_vector.time" with the string "ITime"
    Output
        uint8 int64       date     time a decimal   duration datetime_without_tz    datetime_with_tz
      1     1     1 1970-01-02 00:00:00 1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00:00:00 2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00:00:00 3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Message
      `time` is overridden by the option "polars.to_r_vector.time" with the string "ITime"
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      [1] "00:00:00" "00:00:00" "00:00:00"
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.struct = dataframe

    Code
      series$to_r_vector()
    Output
        uint8 int64       date            time a decimal   duration datetime_without_tz    datetime_with_tz
      1     1     1 1970-01-02 00:00:00.000000 1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00:00:00.000000 2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00:00:00.000000 3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.struct = tibble

    Code
      series$to_r_vector()
    Message
      `struct` is overridden by the option "polars.to_r_vector.struct" with the string "tibble"
    Output
      # A tibble: 3 x 9
        uint8 int64 date       time          struct$a decimal duration   datetime_without_tz datetime_with_tz   
        <int> <dbl> <date>     <time>           <int>   <dbl> <drtn>     <dttm>              <dttm>             
      1     1     1 1970-01-02 00'00.000000"        1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00'00.000000"        2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00'00.000000"        3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Message
      `struct` is overridden by the option "polars.to_r_vector.struct" with the string "tibble"
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
      # A tibble: 3 x 1
            a
        <int>
      1     1
      2     2
      3     3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.decimal = double

    Code
      series$to_r_vector()
    Output
        uint8 int64       date            time a decimal   duration datetime_without_tz    datetime_with_tz
      1     1     1 1970-01-02 00:00:00.000000 1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00:00:00.000000 2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00:00:00.000000 3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.decimal = character

    Code
      series$to_r_vector()
    Message
      `decimal` is overridden by the option "polars.to_r_vector.decimal" with the string "character"
    Output
        uint8 int64       date            time a decimal   duration datetime_without_tz    datetime_with_tz
      1     1     1 1970-01-02 00:00:00.000000 1    1.00 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00:00:00.000000 2    2.00 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00:00:00.000000 3    3.00 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Message
      `decimal` is overridden by the option "polars.to_r_vector.decimal" with the string "character"
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] "1.00" "2.00" "3.00"
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.as_clock_class = FALSE

    Code
      series$to_r_vector()
    Output
        uint8 int64       date            time a decimal   duration datetime_without_tz    datetime_with_tz
      1     1     1 1970-01-02 00:00:00.000000 1       1 0.001 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      2     2     2 1970-01-03 00:00:00.000000 2       2 0.002 secs 1970-01-01 00:00:00 1970-01-01 01:00:00
      3     3     3 1970-01-04 00:00:00.000000 3       3 0.003 secs 1970-01-01 00:00:00 1970-01-01 01:00:00

---

    Code
      as.vector(series)
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      Time differences in secs
      [1] 0.001 0.002 0.003
      
      $datetime_without_tz
      [1] "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC" "1970-01-01 00:00:00 UTC"
      
      $datetime_with_tz
      [1] "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST" "1970-01-01 01:00:00 BST"
      

# options for to_r_vector() works: polars.to_r_vector.as_clock_class = TRUE

    Code
      series$to_r_vector()
    Message
      `as_clock_class` is overridden by the option "polars.to_r_vector.as_clock_class" with `TRUE`
    Output
        uint8 int64       date            time a decimal duration     datetime_without_tz                             datetime_with_tz
      1     1     1 1970-01-02 00:00:00.000000 1       1        1 1970-01-01T00:00:00.001 1970-01-01T01:00:00.001+01:00[Europe/London]
      2     2     2 1970-01-03 00:00:00.000000 2       2        2 1970-01-01T00:00:00.002 1970-01-01T01:00:00.002+01:00[Europe/London]
      3     3     3 1970-01-04 00:00:00.000000 3       3        3 1970-01-01T00:00:00.003 1970-01-01T01:00:00.003+01:00[Europe/London]

---

    Code
      as.vector(series)
    Message
      `as_clock_class` is overridden by the option "polars.to_r_vector.as_clock_class" with `TRUE`
    Output
      $uint8
      [1] 1 2 3
      
      $int64
      [1] 1 2 3
      
      $date
      [1] "1970-01-02" "1970-01-03" "1970-01-04"
      
      $time
      00:00:00.000000
      00:00:00.000000
      00:00:00.000000
      
      $struct
        a
      1 1
      2 2
      3 3
      
      $decimal
      [1] 1 2 3
      
      $duration
      <duration<millisecond>[3]>
      [1] 1 2 3
      
      $datetime_without_tz
      <naive_time<millisecond>[3]>
      [1] "1970-01-01T00:00:00.001" "1970-01-01T00:00:00.002" "1970-01-01T00:00:00.003"
      
      $datetime_with_tz
      <zoned_time<millisecond><Europe/London>[3]>
      [1] "1970-01-01T01:00:00.001+01:00" "1970-01-01T01:00:00.002+01:00" "1970-01-01T01:00:00.003+01:00"
      

