// Copyright (c) 2023-2026 ParadeDB, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use crate::nodecast;
use crate::postgres::var::{find_one_var_and_fieldname, find_var_relation, VarContext};
use chrono::{DateTime, NaiveDate};
use pgrx::datum::datetime_support::DateTimeConversionError;
use pgrx::{pg_sys, PgList};

pub static MICROSECONDS_IN_SECOND: u32 = 1_000_000;

/// The minimum nanoseconds from 1970-01-01 00:00:00 UTC that can be safely
/// converted between Postgres types and Tantivy without underflowing i64 when floored to the
/// day.
#[allow(dead_code)]
pub const MIN_SAFE_TANTIVY_NANOS: i64 =
    (i64::MIN / 1_000_000_000 / 86_400) * 86_400 * 1_000_000_000;

/// The maximum nanoseconds from 1970-01-01 00:00:00 UTC that can be safely
/// converted between Postgres types and Tantivy without overflowing i64 when floored to the
/// day.
#[allow(dead_code)]
pub const MAX_SAFE_TANTIVY_NANOS: i64 =
    (i64::MAX / 1_000_000_000 / 86_400) * 86_400 * 1_000_000_000;

#[inline]
pub fn micros_to_tantivy_datetime(
    micros: i64,
) -> Result<tantivy::DateTime, DateTimeConversionError> {
    let nanos = micros
        .checked_mul(1_000)
        .ok_or(DateTimeConversionError::OutOfRange)?;
    Ok(tantivy::DateTime::from_timestamp_nanos(nanos))
}

pub fn datetime_components_to_tantivy_date(
    ymd: Option<(i32, u8, u8)>,
    hms_micro: (u8, u8, u8, u32),
) -> Result<tantivy::schema::OwnedValue, DateTimeConversionError> {
    let naive_dt = match ymd {
        Some(ymd) => NaiveDate::from_ymd_opt(ymd.0, ymd.1.into(), ymd.2.into())
            .expect("ymd should be valid for NaiveDate::from_ymd_opt"),
        None => DateTime::UNIX_EPOCH.date_naive(),
    }
    .and_hms_micro_opt(
        hms_micro.0.into(),
        hms_micro.1.into(),
        hms_micro.2.into(),
        hms_micro.3 % MICROSECONDS_IN_SECOND,
    )
    .ok_or(DateTimeConversionError::OutOfRange)?
    .and_utc();

    Ok(tantivy::schema::OwnedValue::Date(
        micros_to_tantivy_datetime(naive_dt.timestamp_micros())?,
    ))
}

/// If `expr` is a DATE(column) function call where the inner column
/// is resolvable to a field name, return (field_name, attno).
pub unsafe fn extract_date_func_field(
    var_context: VarContext,
    expr: *mut pg_sys::Node,
    root: *mut pg_sys::PlannerInfo,
) -> Option<(String, pg_sys::AttrNumber)> {
    let func_expr = nodecast!(FuncExpr, T_FuncExpr, expr)?;

    // Check if the result type is DATE — this covers DATE(timestamp_col)
    if (*func_expr).funcresulttype != pg_sys::DATEOID {
        return None;
    }

    // Get the first argument — that's the column being cast
    let args = PgList::<pg_sys::Node>::from_pg((*func_expr).args);
    let inner_arg = args.get_ptr(0)?;

    // Try to resolve the inner arg to a field name
    let (var, field_name) = find_one_var_and_fieldname(var_context, inner_arg)?;
    let (heaprelid, attno, _) = find_var_relation(var, root);
    if heaprelid == pg_sys::InvalidOid {
        return None;
    }

    Some((field_name.to_string(), attno))
}
