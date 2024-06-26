
#include <stdint.h>
#include <Rinternals.h>
#include <R_ext/Parse.h>

#include "rust/api.h"

static uintptr_t TAGGED_POINTER_MASK = (uintptr_t)1;

SEXP handle_result(SEXP res_) {
    uintptr_t res = (uintptr_t)res_;

    // An error is indicated by tag.
    if ((res & TAGGED_POINTER_MASK) == 1) {
        // Remove tag
        SEXP res_aligned = (SEXP)(res & ~TAGGED_POINTER_MASK);

        // Currently, there are two types of error cases:
        //
        //   1. Error from Rust code
        //   2. Error from R's C API, which is caught by R_UnwindProtect()
        //
        if (TYPEOF(res_aligned) == CHARSXP) {
            // In case 1, the result is an error message that can be passed to
            // Rf_errorcall() directly.
            Rf_errorcall(R_NilValue, "%s", CHAR(res_aligned));
        } else {
            // In case 2, the result is the token to restart the
            // cleanup process on R's side.
            R_ContinueUnwind(res_aligned);
        }
    }

    return (SEXP)res;
}

SEXP savvy_col__impl(SEXP name) {
    SEXP res = savvy_col__ffi(name);
    return handle_result(res);
}

SEXP savvy_cols__impl(SEXP names) {
    SEXP res = savvy_cols__ffi(names);
    return handle_result(res);
}

SEXP savvy_lit_from_bool__impl(SEXP value) {
    SEXP res = savvy_lit_from_bool__ffi(value);
    return handle_result(res);
}

SEXP savvy_lit_from_i32__impl(SEXP value) {
    SEXP res = savvy_lit_from_i32__ffi(value);
    return handle_result(res);
}

SEXP savvy_lit_from_f64__impl(SEXP value) {
    SEXP res = savvy_lit_from_f64__ffi(value);
    return handle_result(res);
}

SEXP savvy_lit_from_str__impl(SEXP value) {
    SEXP res = savvy_lit_from_str__ffi(value);
    return handle_result(res);
}

SEXP savvy_lit_null__impl(void) {
    SEXP res = savvy_lit_null__ffi();
    return handle_result(res);
}

SEXP savvy_lit_from_series__impl(SEXP value) {
    SEXP res = savvy_lit_from_series__ffi(value);
    return handle_result(res);
}

SEXP savvy_when__impl(SEXP condition) {
    SEXP res = savvy_when__ffi(condition);
    return handle_result(res);
}

SEXP savvy_PlRChainedThen_when__impl(SEXP self__, SEXP condition) {
    SEXP res = savvy_PlRChainedThen_when__ffi(self__, condition);
    return handle_result(res);
}

SEXP savvy_PlRChainedThen_otherwise__impl(SEXP self__, SEXP statement) {
    SEXP res = savvy_PlRChainedThen_otherwise__ffi(self__, statement);
    return handle_result(res);
}

SEXP savvy_PlRChainedWhen_then__impl(SEXP self__, SEXP statement) {
    SEXP res = savvy_PlRChainedWhen_then__ffi(self__, statement);
    return handle_result(res);
}

SEXP savvy_PlRDataFrame_init__impl(SEXP columns) {
    SEXP res = savvy_PlRDataFrame_init__ffi(columns);
    return handle_result(res);
}

SEXP savvy_PlRDataFrame_print__impl(SEXP self__) {
    SEXP res = savvy_PlRDataFrame_print__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataFrame_get_columns__impl(SEXP self__) {
    SEXP res = savvy_PlRDataFrame_get_columns__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataFrame_shape__impl(SEXP self__) {
    SEXP res = savvy_PlRDataFrame_shape__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataFrame_height__impl(SEXP self__) {
    SEXP res = savvy_PlRDataFrame_height__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataFrame_width__impl(SEXP self__) {
    SEXP res = savvy_PlRDataFrame_width__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataFrame_to_series__impl(SEXP self__, SEXP index) {
    SEXP res = savvy_PlRDataFrame_to_series__ffi(self__, index);
    return handle_result(res);
}

SEXP savvy_PlRDataFrame_to_struct__impl(SEXP self__, SEXP name) {
    SEXP res = savvy_PlRDataFrame_to_struct__ffi(self__, name);
    return handle_result(res);
}

SEXP savvy_PlRDataFrame_lazy__impl(SEXP self__) {
    SEXP res = savvy_PlRDataFrame_lazy__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_print__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_print__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_new_from_name__impl(SEXP name) {
    SEXP res = savvy_PlRDataType_new_from_name__ffi(name);
    return handle_result(res);
}

SEXP savvy_PlRDataType_new_categorical__impl(SEXP ordering) {
    SEXP res = savvy_PlRDataType_new_categorical__ffi(ordering);
    return handle_result(res);
}

SEXP savvy_PlRDataType_new_datetime__impl(SEXP time_unit, SEXP time_zone) {
    SEXP res = savvy_PlRDataType_new_datetime__ffi(time_unit, time_zone);
    return handle_result(res);
}

SEXP savvy_PlRDataType_new_duration__impl(SEXP time_unit) {
    SEXP res = savvy_PlRDataType_new_duration__ffi(time_unit);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_temporal__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_temporal__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_enum__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_enum__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_categorical__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_categorical__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_string__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_string__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_logical__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_logical__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_float__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_float__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_numeric__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_numeric__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_integer__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_integer__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_signed_integer__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_signed_integer__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_unsigned_integer__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_unsigned_integer__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_null__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_null__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_binary__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_binary__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_primitive__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_primitive__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_bool__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_bool__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_array__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_array__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_list__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_list__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_nested__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_nested__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_struct__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_struct__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_ord__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_ord__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRDataType_is_known__impl(SEXP self__) {
    SEXP res = savvy_PlRDataType_is_known__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_dt_convert_time_zone__impl(SEXP self__, SEXP time_zone) {
    SEXP res = savvy_PlRExpr_dt_convert_time_zone__ffi(self__, time_zone);
    return handle_result(res);
}

SEXP savvy_PlRExpr_dt_replace_time_zone__impl(SEXP self__, SEXP ambiguous, SEXP non_existent, SEXP time_zone) {
    SEXP res = savvy_PlRExpr_dt_replace_time_zone__ffi(self__, ambiguous, non_existent, time_zone);
    return handle_result(res);
}

SEXP savvy_PlRExpr_print__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_print__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_add__impl(SEXP self__, SEXP rhs) {
    SEXP res = savvy_PlRExpr_add__ffi(self__, rhs);
    return handle_result(res);
}

SEXP savvy_PlRExpr_sub__impl(SEXP self__, SEXP rhs) {
    SEXP res = savvy_PlRExpr_sub__ffi(self__, rhs);
    return handle_result(res);
}

SEXP savvy_PlRExpr_mul__impl(SEXP self__, SEXP rhs) {
    SEXP res = savvy_PlRExpr_mul__ffi(self__, rhs);
    return handle_result(res);
}

SEXP savvy_PlRExpr_div__impl(SEXP self__, SEXP rhs) {
    SEXP res = savvy_PlRExpr_div__ffi(self__, rhs);
    return handle_result(res);
}

SEXP savvy_PlRExpr_rem__impl(SEXP self__, SEXP rhs) {
    SEXP res = savvy_PlRExpr_rem__ffi(self__, rhs);
    return handle_result(res);
}

SEXP savvy_PlRExpr_floor_div__impl(SEXP self__, SEXP rhs) {
    SEXP res = savvy_PlRExpr_floor_div__ffi(self__, rhs);
    return handle_result(res);
}

SEXP savvy_PlRExpr_neg__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_neg__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_eq__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_eq__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_eq_missing__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_eq_missing__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_neq__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_neq__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_neq_missing__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_neq_missing__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_gt__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_gt__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_gt_eq__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_gt_eq__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_lt_eq__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_lt_eq__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_lt__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_lt__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_alias__impl(SEXP self__, SEXP name) {
    SEXP res = savvy_PlRExpr_alias__ffi(self__, name);
    return handle_result(res);
}

SEXP savvy_PlRExpr_not__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_not__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_is_null__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_is_null__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_is_not_null__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_is_not_null__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_is_infinite__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_is_infinite__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_is_finite__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_is_finite__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_is_nan__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_is_nan__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_is_not_nan__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_is_not_nan__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_min__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_min__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_max__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_max__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_nan_max__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_nan_max__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_nan_min__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_nan_min__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_mean__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_mean__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_median__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_median__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_sum__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_sum__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_cast__impl(SEXP self__, SEXP data_type, SEXP strict) {
    SEXP res = savvy_PlRExpr_cast__ffi(self__, data_type, strict);
    return handle_result(res);
}

SEXP savvy_PlRExpr_and__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_and__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_or__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_or__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_xor__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_xor__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_reshape__impl(SEXP self__, SEXP dimensions) {
    SEXP res = savvy_PlRExpr_reshape__ffi(self__, dimensions);
    return handle_result(res);
}

SEXP savvy_PlRExpr_any__impl(SEXP self__, SEXP ignore_nulls) {
    SEXP res = savvy_PlRExpr_any__ffi(self__, ignore_nulls);
    return handle_result(res);
}

SEXP savvy_PlRExpr_all__impl(SEXP self__, SEXP ignore_nulls) {
    SEXP res = savvy_PlRExpr_all__ffi(self__, ignore_nulls);
    return handle_result(res);
}

SEXP savvy_PlRExpr_meta_selector_add__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_meta_selector_add__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_meta_selector_and__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_meta_selector_and__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_meta_selector_sub__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRExpr_meta_selector_sub__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRExpr_meta_as_selector__impl(SEXP self__) {
    SEXP res = savvy_PlRExpr_meta_as_selector__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRExpr_struct_field_by_index__impl(SEXP self__, SEXP index) {
    SEXP res = savvy_PlRExpr_struct_field_by_index__ffi(self__, index);
    return handle_result(res);
}

SEXP savvy_PlRExpr_struct_field_by_name__impl(SEXP self__, SEXP name) {
    SEXP res = savvy_PlRExpr_struct_field_by_name__ffi(self__, name);
    return handle_result(res);
}

SEXP savvy_PlRLazyFrame_select__impl(SEXP self__, SEXP exprs) {
    SEXP res = savvy_PlRLazyFrame_select__ffi(self__, exprs);
    return handle_result(res);
}

SEXP savvy_PlRLazyFrame_group_by__impl(SEXP self__, SEXP by, SEXP maintain_order) {
    SEXP res = savvy_PlRLazyFrame_group_by__ffi(self__, by, maintain_order);
    return handle_result(res);
}

SEXP savvy_PlRLazyFrame_collect__impl(SEXP self__) {
    SEXP res = savvy_PlRLazyFrame_collect__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRLazyGroupBy_agg__impl(SEXP self__, SEXP aggs) {
    SEXP res = savvy_PlRLazyGroupBy_agg__ffi(self__, aggs);
    return handle_result(res);
}

SEXP savvy_PlRLazyGroupBy_head__impl(SEXP self__, SEXP n) {
    SEXP res = savvy_PlRLazyGroupBy_head__ffi(self__, n);
    return handle_result(res);
}

SEXP savvy_PlRLazyGroupBy_tail__impl(SEXP self__, SEXP n) {
    SEXP res = savvy_PlRLazyGroupBy_tail__ffi(self__, n);
    return handle_result(res);
}

SEXP savvy_PlRSeries_print__impl(SEXP self__) {
    SEXP res = savvy_PlRSeries_print__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRSeries_struct_unnest__impl(SEXP self__) {
    SEXP res = savvy_PlRSeries_struct_unnest__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRSeries_struct_fields__impl(SEXP self__) {
    SEXP res = savvy_PlRSeries_struct_fields__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRSeries_reshape__impl(SEXP self__, SEXP dimensions) {
    SEXP res = savvy_PlRSeries_reshape__ffi(self__, dimensions);
    return handle_result(res);
}

SEXP savvy_PlRSeries_clone__impl(SEXP self__) {
    SEXP res = savvy_PlRSeries_clone__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRSeries_name__impl(SEXP self__) {
    SEXP res = savvy_PlRSeries_name__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRSeries_rename__impl(SEXP self__, SEXP name) {
    SEXP res = savvy_PlRSeries_rename__ffi(self__, name);
    return handle_result(res);
}

SEXP savvy_PlRSeries_dtype__impl(SEXP self__) {
    SEXP res = savvy_PlRSeries_dtype__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRSeries_cast__impl(SEXP self__, SEXP dtype, SEXP strict) {
    SEXP res = savvy_PlRSeries_cast__ffi(self__, dtype, strict);
    return handle_result(res);
}

SEXP savvy_PlRSeries_add__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRSeries_add__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRSeries_sub__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRSeries_sub__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRSeries_div__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRSeries_div__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRSeries_mul__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRSeries_mul__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRSeries_rem__impl(SEXP self__, SEXP other) {
    SEXP res = savvy_PlRSeries_rem__ffi(self__, other);
    return handle_result(res);
}

SEXP savvy_PlRSeries_new_empty__impl(SEXP name, SEXP dtype) {
    SEXP res = savvy_PlRSeries_new_empty__ffi(name, dtype);
    return handle_result(res);
}

SEXP savvy_PlRSeries_new_f64__impl(SEXP name, SEXP values) {
    SEXP res = savvy_PlRSeries_new_f64__ffi(name, values);
    return handle_result(res);
}

SEXP savvy_PlRSeries_new_i32__impl(SEXP name, SEXP values) {
    SEXP res = savvy_PlRSeries_new_i32__ffi(name, values);
    return handle_result(res);
}

SEXP savvy_PlRSeries_new_bool__impl(SEXP name, SEXP values) {
    SEXP res = savvy_PlRSeries_new_bool__ffi(name, values);
    return handle_result(res);
}

SEXP savvy_PlRSeries_new_str__impl(SEXP name, SEXP values) {
    SEXP res = savvy_PlRSeries_new_str__ffi(name, values);
    return handle_result(res);
}

SEXP savvy_PlRSeries_new_series_list__impl(SEXP name, SEXP values) {
    SEXP res = savvy_PlRSeries_new_series_list__ffi(name, values);
    return handle_result(res);
}

SEXP savvy_PlRSeries_to_r_vector__impl(SEXP self__) {
    SEXP res = savvy_PlRSeries_to_r_vector__ffi(self__);
    return handle_result(res);
}

SEXP savvy_PlRThen_when__impl(SEXP self__, SEXP condition) {
    SEXP res = savvy_PlRThen_when__ffi(self__, condition);
    return handle_result(res);
}

SEXP savvy_PlRThen_otherwise__impl(SEXP self__, SEXP statement) {
    SEXP res = savvy_PlRThen_otherwise__ffi(self__, statement);
    return handle_result(res);
}

SEXP savvy_PlRWhen_then__impl(SEXP self__, SEXP statement) {
    SEXP res = savvy_PlRWhen_then__ffi(self__, statement);
    return handle_result(res);
}


static const R_CallMethodDef CallEntries[] = {
    {"savvy_col__impl", (DL_FUNC) &savvy_col__impl, 1},
    {"savvy_cols__impl", (DL_FUNC) &savvy_cols__impl, 1},
    {"savvy_lit_from_bool__impl", (DL_FUNC) &savvy_lit_from_bool__impl, 1},
    {"savvy_lit_from_i32__impl", (DL_FUNC) &savvy_lit_from_i32__impl, 1},
    {"savvy_lit_from_f64__impl", (DL_FUNC) &savvy_lit_from_f64__impl, 1},
    {"savvy_lit_from_str__impl", (DL_FUNC) &savvy_lit_from_str__impl, 1},
    {"savvy_lit_null__impl", (DL_FUNC) &savvy_lit_null__impl, 0},
    {"savvy_lit_from_series__impl", (DL_FUNC) &savvy_lit_from_series__impl, 1},
    {"savvy_when__impl", (DL_FUNC) &savvy_when__impl, 1},
    {"savvy_PlRChainedThen_when__impl", (DL_FUNC) &savvy_PlRChainedThen_when__impl, 2},
    {"savvy_PlRChainedThen_otherwise__impl", (DL_FUNC) &savvy_PlRChainedThen_otherwise__impl, 2},
    {"savvy_PlRChainedWhen_then__impl", (DL_FUNC) &savvy_PlRChainedWhen_then__impl, 2},
    {"savvy_PlRDataFrame_init__impl", (DL_FUNC) &savvy_PlRDataFrame_init__impl, 1},
    {"savvy_PlRDataFrame_print__impl", (DL_FUNC) &savvy_PlRDataFrame_print__impl, 1},
    {"savvy_PlRDataFrame_get_columns__impl", (DL_FUNC) &savvy_PlRDataFrame_get_columns__impl, 1},
    {"savvy_PlRDataFrame_shape__impl", (DL_FUNC) &savvy_PlRDataFrame_shape__impl, 1},
    {"savvy_PlRDataFrame_height__impl", (DL_FUNC) &savvy_PlRDataFrame_height__impl, 1},
    {"savvy_PlRDataFrame_width__impl", (DL_FUNC) &savvy_PlRDataFrame_width__impl, 1},
    {"savvy_PlRDataFrame_to_series__impl", (DL_FUNC) &savvy_PlRDataFrame_to_series__impl, 2},
    {"savvy_PlRDataFrame_to_struct__impl", (DL_FUNC) &savvy_PlRDataFrame_to_struct__impl, 2},
    {"savvy_PlRDataFrame_lazy__impl", (DL_FUNC) &savvy_PlRDataFrame_lazy__impl, 1},
    {"savvy_PlRDataType_print__impl", (DL_FUNC) &savvy_PlRDataType_print__impl, 1},
    {"savvy_PlRDataType_new_from_name__impl", (DL_FUNC) &savvy_PlRDataType_new_from_name__impl, 1},
    {"savvy_PlRDataType_new_categorical__impl", (DL_FUNC) &savvy_PlRDataType_new_categorical__impl, 1},
    {"savvy_PlRDataType_new_datetime__impl", (DL_FUNC) &savvy_PlRDataType_new_datetime__impl, 2},
    {"savvy_PlRDataType_new_duration__impl", (DL_FUNC) &savvy_PlRDataType_new_duration__impl, 1},
    {"savvy_PlRDataType_is_temporal__impl", (DL_FUNC) &savvy_PlRDataType_is_temporal__impl, 1},
    {"savvy_PlRDataType_is_enum__impl", (DL_FUNC) &savvy_PlRDataType_is_enum__impl, 1},
    {"savvy_PlRDataType_is_categorical__impl", (DL_FUNC) &savvy_PlRDataType_is_categorical__impl, 1},
    {"savvy_PlRDataType_is_string__impl", (DL_FUNC) &savvy_PlRDataType_is_string__impl, 1},
    {"savvy_PlRDataType_is_logical__impl", (DL_FUNC) &savvy_PlRDataType_is_logical__impl, 1},
    {"savvy_PlRDataType_is_float__impl", (DL_FUNC) &savvy_PlRDataType_is_float__impl, 1},
    {"savvy_PlRDataType_is_numeric__impl", (DL_FUNC) &savvy_PlRDataType_is_numeric__impl, 1},
    {"savvy_PlRDataType_is_integer__impl", (DL_FUNC) &savvy_PlRDataType_is_integer__impl, 1},
    {"savvy_PlRDataType_is_signed_integer__impl", (DL_FUNC) &savvy_PlRDataType_is_signed_integer__impl, 1},
    {"savvy_PlRDataType_is_unsigned_integer__impl", (DL_FUNC) &savvy_PlRDataType_is_unsigned_integer__impl, 1},
    {"savvy_PlRDataType_is_null__impl", (DL_FUNC) &savvy_PlRDataType_is_null__impl, 1},
    {"savvy_PlRDataType_is_binary__impl", (DL_FUNC) &savvy_PlRDataType_is_binary__impl, 1},
    {"savvy_PlRDataType_is_primitive__impl", (DL_FUNC) &savvy_PlRDataType_is_primitive__impl, 1},
    {"savvy_PlRDataType_is_bool__impl", (DL_FUNC) &savvy_PlRDataType_is_bool__impl, 1},
    {"savvy_PlRDataType_is_array__impl", (DL_FUNC) &savvy_PlRDataType_is_array__impl, 1},
    {"savvy_PlRDataType_is_list__impl", (DL_FUNC) &savvy_PlRDataType_is_list__impl, 1},
    {"savvy_PlRDataType_is_nested__impl", (DL_FUNC) &savvy_PlRDataType_is_nested__impl, 1},
    {"savvy_PlRDataType_is_struct__impl", (DL_FUNC) &savvy_PlRDataType_is_struct__impl, 1},
    {"savvy_PlRDataType_is_ord__impl", (DL_FUNC) &savvy_PlRDataType_is_ord__impl, 1},
    {"savvy_PlRDataType_is_known__impl", (DL_FUNC) &savvy_PlRDataType_is_known__impl, 1},
    {"savvy_PlRExpr_dt_convert_time_zone__impl", (DL_FUNC) &savvy_PlRExpr_dt_convert_time_zone__impl, 2},
    {"savvy_PlRExpr_dt_replace_time_zone__impl", (DL_FUNC) &savvy_PlRExpr_dt_replace_time_zone__impl, 4},
    {"savvy_PlRExpr_print__impl", (DL_FUNC) &savvy_PlRExpr_print__impl, 1},
    {"savvy_PlRExpr_add__impl", (DL_FUNC) &savvy_PlRExpr_add__impl, 2},
    {"savvy_PlRExpr_sub__impl", (DL_FUNC) &savvy_PlRExpr_sub__impl, 2},
    {"savvy_PlRExpr_mul__impl", (DL_FUNC) &savvy_PlRExpr_mul__impl, 2},
    {"savvy_PlRExpr_div__impl", (DL_FUNC) &savvy_PlRExpr_div__impl, 2},
    {"savvy_PlRExpr_rem__impl", (DL_FUNC) &savvy_PlRExpr_rem__impl, 2},
    {"savvy_PlRExpr_floor_div__impl", (DL_FUNC) &savvy_PlRExpr_floor_div__impl, 2},
    {"savvy_PlRExpr_neg__impl", (DL_FUNC) &savvy_PlRExpr_neg__impl, 1},
    {"savvy_PlRExpr_eq__impl", (DL_FUNC) &savvy_PlRExpr_eq__impl, 2},
    {"savvy_PlRExpr_eq_missing__impl", (DL_FUNC) &savvy_PlRExpr_eq_missing__impl, 2},
    {"savvy_PlRExpr_neq__impl", (DL_FUNC) &savvy_PlRExpr_neq__impl, 2},
    {"savvy_PlRExpr_neq_missing__impl", (DL_FUNC) &savvy_PlRExpr_neq_missing__impl, 2},
    {"savvy_PlRExpr_gt__impl", (DL_FUNC) &savvy_PlRExpr_gt__impl, 2},
    {"savvy_PlRExpr_gt_eq__impl", (DL_FUNC) &savvy_PlRExpr_gt_eq__impl, 2},
    {"savvy_PlRExpr_lt_eq__impl", (DL_FUNC) &savvy_PlRExpr_lt_eq__impl, 2},
    {"savvy_PlRExpr_lt__impl", (DL_FUNC) &savvy_PlRExpr_lt__impl, 2},
    {"savvy_PlRExpr_alias__impl", (DL_FUNC) &savvy_PlRExpr_alias__impl, 2},
    {"savvy_PlRExpr_not__impl", (DL_FUNC) &savvy_PlRExpr_not__impl, 1},
    {"savvy_PlRExpr_is_null__impl", (DL_FUNC) &savvy_PlRExpr_is_null__impl, 1},
    {"savvy_PlRExpr_is_not_null__impl", (DL_FUNC) &savvy_PlRExpr_is_not_null__impl, 1},
    {"savvy_PlRExpr_is_infinite__impl", (DL_FUNC) &savvy_PlRExpr_is_infinite__impl, 1},
    {"savvy_PlRExpr_is_finite__impl", (DL_FUNC) &savvy_PlRExpr_is_finite__impl, 1},
    {"savvy_PlRExpr_is_nan__impl", (DL_FUNC) &savvy_PlRExpr_is_nan__impl, 1},
    {"savvy_PlRExpr_is_not_nan__impl", (DL_FUNC) &savvy_PlRExpr_is_not_nan__impl, 1},
    {"savvy_PlRExpr_min__impl", (DL_FUNC) &savvy_PlRExpr_min__impl, 1},
    {"savvy_PlRExpr_max__impl", (DL_FUNC) &savvy_PlRExpr_max__impl, 1},
    {"savvy_PlRExpr_nan_max__impl", (DL_FUNC) &savvy_PlRExpr_nan_max__impl, 1},
    {"savvy_PlRExpr_nan_min__impl", (DL_FUNC) &savvy_PlRExpr_nan_min__impl, 1},
    {"savvy_PlRExpr_mean__impl", (DL_FUNC) &savvy_PlRExpr_mean__impl, 1},
    {"savvy_PlRExpr_median__impl", (DL_FUNC) &savvy_PlRExpr_median__impl, 1},
    {"savvy_PlRExpr_sum__impl", (DL_FUNC) &savvy_PlRExpr_sum__impl, 1},
    {"savvy_PlRExpr_cast__impl", (DL_FUNC) &savvy_PlRExpr_cast__impl, 3},
    {"savvy_PlRExpr_and__impl", (DL_FUNC) &savvy_PlRExpr_and__impl, 2},
    {"savvy_PlRExpr_or__impl", (DL_FUNC) &savvy_PlRExpr_or__impl, 2},
    {"savvy_PlRExpr_xor__impl", (DL_FUNC) &savvy_PlRExpr_xor__impl, 2},
    {"savvy_PlRExpr_reshape__impl", (DL_FUNC) &savvy_PlRExpr_reshape__impl, 2},
    {"savvy_PlRExpr_any__impl", (DL_FUNC) &savvy_PlRExpr_any__impl, 2},
    {"savvy_PlRExpr_all__impl", (DL_FUNC) &savvy_PlRExpr_all__impl, 2},
    {"savvy_PlRExpr_meta_selector_add__impl", (DL_FUNC) &savvy_PlRExpr_meta_selector_add__impl, 2},
    {"savvy_PlRExpr_meta_selector_and__impl", (DL_FUNC) &savvy_PlRExpr_meta_selector_and__impl, 2},
    {"savvy_PlRExpr_meta_selector_sub__impl", (DL_FUNC) &savvy_PlRExpr_meta_selector_sub__impl, 2},
    {"savvy_PlRExpr_meta_as_selector__impl", (DL_FUNC) &savvy_PlRExpr_meta_as_selector__impl, 1},
    {"savvy_PlRExpr_struct_field_by_index__impl", (DL_FUNC) &savvy_PlRExpr_struct_field_by_index__impl, 2},
    {"savvy_PlRExpr_struct_field_by_name__impl", (DL_FUNC) &savvy_PlRExpr_struct_field_by_name__impl, 2},
    {"savvy_PlRLazyFrame_select__impl", (DL_FUNC) &savvy_PlRLazyFrame_select__impl, 2},
    {"savvy_PlRLazyFrame_group_by__impl", (DL_FUNC) &savvy_PlRLazyFrame_group_by__impl, 3},
    {"savvy_PlRLazyFrame_collect__impl", (DL_FUNC) &savvy_PlRLazyFrame_collect__impl, 1},
    {"savvy_PlRLazyGroupBy_agg__impl", (DL_FUNC) &savvy_PlRLazyGroupBy_agg__impl, 2},
    {"savvy_PlRLazyGroupBy_head__impl", (DL_FUNC) &savvy_PlRLazyGroupBy_head__impl, 2},
    {"savvy_PlRLazyGroupBy_tail__impl", (DL_FUNC) &savvy_PlRLazyGroupBy_tail__impl, 2},
    {"savvy_PlRSeries_print__impl", (DL_FUNC) &savvy_PlRSeries_print__impl, 1},
    {"savvy_PlRSeries_struct_unnest__impl", (DL_FUNC) &savvy_PlRSeries_struct_unnest__impl, 1},
    {"savvy_PlRSeries_struct_fields__impl", (DL_FUNC) &savvy_PlRSeries_struct_fields__impl, 1},
    {"savvy_PlRSeries_reshape__impl", (DL_FUNC) &savvy_PlRSeries_reshape__impl, 2},
    {"savvy_PlRSeries_clone__impl", (DL_FUNC) &savvy_PlRSeries_clone__impl, 1},
    {"savvy_PlRSeries_name__impl", (DL_FUNC) &savvy_PlRSeries_name__impl, 1},
    {"savvy_PlRSeries_rename__impl", (DL_FUNC) &savvy_PlRSeries_rename__impl, 2},
    {"savvy_PlRSeries_dtype__impl", (DL_FUNC) &savvy_PlRSeries_dtype__impl, 1},
    {"savvy_PlRSeries_cast__impl", (DL_FUNC) &savvy_PlRSeries_cast__impl, 3},
    {"savvy_PlRSeries_add__impl", (DL_FUNC) &savvy_PlRSeries_add__impl, 2},
    {"savvy_PlRSeries_sub__impl", (DL_FUNC) &savvy_PlRSeries_sub__impl, 2},
    {"savvy_PlRSeries_div__impl", (DL_FUNC) &savvy_PlRSeries_div__impl, 2},
    {"savvy_PlRSeries_mul__impl", (DL_FUNC) &savvy_PlRSeries_mul__impl, 2},
    {"savvy_PlRSeries_rem__impl", (DL_FUNC) &savvy_PlRSeries_rem__impl, 2},
    {"savvy_PlRSeries_new_empty__impl", (DL_FUNC) &savvy_PlRSeries_new_empty__impl, 2},
    {"savvy_PlRSeries_new_f64__impl", (DL_FUNC) &savvy_PlRSeries_new_f64__impl, 2},
    {"savvy_PlRSeries_new_i32__impl", (DL_FUNC) &savvy_PlRSeries_new_i32__impl, 2},
    {"savvy_PlRSeries_new_bool__impl", (DL_FUNC) &savvy_PlRSeries_new_bool__impl, 2},
    {"savvy_PlRSeries_new_str__impl", (DL_FUNC) &savvy_PlRSeries_new_str__impl, 2},
    {"savvy_PlRSeries_new_series_list__impl", (DL_FUNC) &savvy_PlRSeries_new_series_list__impl, 2},
    {"savvy_PlRSeries_to_r_vector__impl", (DL_FUNC) &savvy_PlRSeries_to_r_vector__impl, 1},
    {"savvy_PlRThen_when__impl", (DL_FUNC) &savvy_PlRThen_when__impl, 2},
    {"savvy_PlRThen_otherwise__impl", (DL_FUNC) &savvy_PlRThen_otherwise__impl, 2},
    {"savvy_PlRWhen_then__impl", (DL_FUNC) &savvy_PlRWhen_then__impl, 2},
    {NULL, NULL, 0}
};

void R_init_neopolars(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);

    // Functions for initialzation, if any.

}
