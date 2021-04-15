# -*- coding:utf-8 -*-
from past.builtins import unicode
import logging
import re
from bson import ObjectId
import pandas as pd
import dask.dataframe as dd
from numba import jit
from datahub_datacook import common_cook
from ..blitz.dataframe_utils import is_dask_df, dd_concat, to_dd, to_df, df_is_empty, dd_to_parquet, df_size, \
    is_df_like, is_series_like
from ..blitz.exceptions import CookSyntaxException, BlitzRuntimeException, BlitzDatasetIsNull
from ..blitz.misc import path_join, validate_dd_dataset_dir
from yoda.datetime import prc_from_today_int, from_prc_datetime_str, prcnow
from yoda.convert import to_int, to_float, to_unicode_if_string
from .lru import lru_cache
import numpy as np

logger = logging.getLogger(__name__)

# make alias

# parse_dateint = common_cook.parse_dateint

parse_datetime = common_cook.parse_datetime

parse_bool = common_cook.parse_bool

parse_time_interval = common_cook.parse_time_interval

current_month_int = common_cook.current_month_int
current_year_int = common_cook.current_year_int
current_day_int = common_cook.today_int
prev_month_int = common_cook.prev_month
next_month_int = common_cook.next_month
parse_prc_datetime = common_cook.parse_datetime
flip_dict = common_cook.flip_dict

_cat = u','.join


small_lru_cache = lru_cache(50)
common_lru_cache = lru_cache(200)
large_lru_cache = lru_cache(1000)


# @large_lru_cache
def hour_minute_int(time_str, splitter=u':'):
    # return datetime.datetime.strptime(str, "%H:%M:%S")
    if not time_str:
        return 0
    time_str = unicode(time_str)
    if splitter not in time_str:
        return 0
    #
    if len(time_str) >= 8:
        hour, minute = time_str[-8:][:5].split(splitter)
    else:
        hour, minute = time_str[:5].split(splitter)
    return int('{:02d}{:02d}'.format(int(hour), int(minute)))


def parse_prc_date_weekday(v):
    v = parse_prc_datetime(v)
    if not v:
        return v
    return v.weekday()


_valid_date_formats = [
    'YYYY-MM-DD',
    'YYYY/MM/DD',
    'YYYY-M-D',
    'YYYY/M/D',
    'YY-MM-DD',
    'YY/MM/DD',
    'YY-M-D',
    'YY/M/D',
    'MM-DD-YY',
    'MM/DD/YY',
    'M/D/YY',
    'YYYY-MM-DD HH:mm:ss',
]


@large_lru_cache
def _parse_to_date_int(v, format=None):
    """
    Parse a date like string into date-int

    :param v:
    :type v:
    :param format:
    :type format:
    :return:
    :rtype:
    """
    if is_nan(v):
        return 0
    if not v:
        return 0
    if isinstance(format, list):
        formats = [_ for _ in format if _ in _valid_date_formats]
        if not formats:
            formats = _valid_date_formats
    elif format in _valid_date_formats:
        formats = [format]
    else:
        logger.warn(u'format<%s> not registered ...', format)
        formats = _valid_date_formats

    for f in formats:
        try:
            v = from_prc_datetime_str(v, f)
            return int(v.format(u'YYYYMMDD'))
        except Exception:
            pass
    return 0


@small_lru_cache
def _parse_month_int(v):
    if is_nan(v):
        return 0
    v = u'{}'.format(v)
    try:
        for _ in (u'/', u'-'):
            if _ in v:
                v = v.replace(_, u'')
        return int(v[:6])
    except ValueError:
        return 0


# def parse_day_column(df, column):
#     if is_dask_df(df):
#         return df[column].apply(_parse_to_date_int, meta=(column, 'int64'))
#     return df[column].apply(_parse_to_date_int)
#

def set_month_column(df, src_column, dest_column=None, ctx=None, *args, **kwargs):
    # print(u'set_month_column, df<%s>' % _cat(df.columns))
    if not dest_column:
        dest_column = src_column
    if is_dask_df(df):
        df[dest_column] = df[src_column].apply(_parse_month_int, meta=(dest_column, 'int64'))
    else:
        df[dest_column] = df[src_column].apply(_parse_month_int)
    return df


def set_date_column(df, src_column, dest_column=None, ctx=None, format=None, *args, **kwargs):
    if not dest_column:
        dest_column = src_column
    if is_dask_df(df):
        df[dest_column] = df[src_column].apply(_parse_to_date_int, meta=(dest_column, 'int64'), format=format)
    else:
        df[dest_column] = df[src_column].apply(_parse_to_date_int, format=format)

    return df


def convert_month_day_time_column(df, src_column, month_column=None, day_column=None, time_column=None, ctx=None, *args,
                                  **kwargs):
    # df_times = df[column].persist()

    # WHY?:
    # Pandas timeindex is not timezone aware, just UTC-naive datetime, here is patch to resolve
    # force naive datetime localize to UTC then convert to PRC timezone aware
    is_dask = is_dask_df(df)
    df_times = df[src_column].dt.tz_localize('UTC').dt.tz_convert('PRC')
    if month_column:
        df[month_column] = df_times.dt.strftime('%Y%m')
        if is_dask:
            df[month_column] = df[month_column].apply(to_int, meta=(month_column, 'int64'))
        else:
            df[month_column] = df[month_column].astype('int64', errors='ignore').fillna(0)
    if day_column:
        df[day_column] = df_times.dt.strftime('%Y%m%d')
        if is_dask:
            df[day_column] = df[day_column].apply(to_int, meta=(day_column, 'int64'))
        else:
            df[day_column] = df[day_column].astype('int64', errors='ignore').fillna(0)
    if time_column:
        df[time_column] = df_times.dt.strftime('%H%M')
        if is_dask:
            df[time_column] = df[time_column].apply(to_int, meta=(time_column, 'int64'))
        else:
            df[time_column] = df[time_column].astype('int64', errors='ignore').fillna(0)
    return df


def quick_weekday_fn(day_int, month_int):
    """
    Quick compute weekday

    :return:
    :rtype:
    """
    first_day_int = int(u'%s01' % month_int)
    d = prc_from_today_int(first_day_int).datetime.isoweekday()
    return ((day_int - first_day_int) % 7 + d) % 7


@jit("int32(int32,int32,int32)", nopython=True, cache=True)
def _compute_weekday_fn(v1, v2, v3):
    return ((v1 - v2) % 7 + v3) % 7


def quick_weekday_fn_v2(month_int):
    """
    Quick compute weekday

    :return:
    :rtype:
    """
    first_day_int = int(u'%s01' % month_int)
    d = prc_from_today_int(first_day_int).datetime.isoweekday()

    @small_lru_cache
    def _wrap(day_int):
        return _compute_weekday_fn(day_int, first_day_int, d)

    return _wrap


def set_weekday_column(df, src_column, dest_column, month_int=None, ctx=None):
    if month_int is None:
        month_int = ctx.month
    if is_dask_df(df):
        df[dest_column] = df[src_column].apply(quick_weekday_fn_v2(month_int), meta=(dest_column, 'int64'))
    else:
        df[dest_column] = df[src_column].apply(quick_weekday_fn_v2(month_int))
    return df


def to_list(v):
    if isinstance(v, list):
        return v
    return [v]


def is_list(v):
    return isinstance(v, list)


def _ensure_cols(df, columns):
    result = set(list(df.columns)) & set(to_list(columns))
    return list(result)


def fetch_cols(df, columns, ctx=None, *args, **kwargs):
    return df[_ensure_cols(df, columns)]


def parse_number_cols(df, columns, ctx=None, *args, **kwargs):
    for _ in _ensure_cols(df, columns):
        df[_] = df[_].apply(common_cook.parse_number)
    return df


def drop_duplicates(df, subset, keep=u'first', ctx=None, *args, **kwargs):
    return df.drop_duplicates(subset=subset, keep=keep)


def add_cols(df, col_map, ctx=None, *args, **kwargs):
    for column, v in col_map.items():
        if isinstance(v, str):
            v = to_unicode_if_string(v)
        df[column] = v
    return df


def is_nan(v):
    return pd.isna(v) or unicode(v).lower() in (u'nan', u'#value!')


# @jit
def _cast_time(v):
    if is_nan(v):
        return 0
    return int(v.strftime('%H%M'))


def parse_time_span_cols(df, columns, ctx=None, *args, **kwargs):
    for _ in _ensure_cols(df, columns):
        col_name = u'{}_time'.format(_)
        if df[_].dtype == u'datetime64[ns]':
            df_times = df[_].dt.tz_localize('UTC').dt.tz_convert('PRC')
            if is_dask_df(df):
                df[col_name] = df_times.apply(_cast_time, meta=(col_name, u'int64'))
            else:
                df[col_name] = df_times.apply(_cast_time)
        else:
            if is_dask_df(df):
                df[col_name] = df[_].apply(hour_minute_int, meta=(col_name, u'int64'))
            else:
                df[col_name] = df[_].apply(hour_minute_int)
    return df


def pad_cols(df, columns, default_value=None, ctx=None, *args, **kwargs):
    columns = to_list(columns)
    result = set(columns) - set(list(df.columns))
    result = list(result)
    if not result:
        return df
    for _ in result:
        df[_] = default_value
    return df


def str_strip_column(df, column, char=u"'", ctx=None, *args, **kwargs):
    df[column] = df[column].str.strip(char)
    return df


def set_meta_month_column(df, column, ctx=None, *args, **kwargs):
    df[column] = ctx.month
    return df


def set_meta_days_column(df, column, ctx=None, *args, **kwargs):
    days = common_cook.to_month_days(ctx.month)
    df[column] = days
    return df


_column_token_re = re.compile(r'(\[\w+\])', re.U)


def df_select(df, query, params=None, ctx=None, persist=False, *args, **kwargs):
    if not query:
        return df
    column_tokens = _column_token_re.findall(query)
    # print(u'===> %s' % df.columns)
    # print(u'====> tokens: %s' % column_tokens)
    cn_cols = {}
    if column_tokens:
        for _ in column_tokens:
            c_name = _.strip(u'[]')
            if c_name not in cn_cols:
                cn_cols[c_name] = u'v_{}'.format(ObjectId())
            query = query.replace(_, cn_cols[c_name])
            # print(u'---> <%s> <%s>' % (_, cn_cols[c_name]))
    if cn_cols:
        df = df.rename(columns=cn_cols)
    if params is None:
        params = {}
    # print(u'===> %s' % df.columns)
    df = df.query(query, local_dict=params, global_dict=ctx.global_vars)
    if cn_cols:
        df = df.rename(columns=flip_dict(cn_cols))
    if persist:
        client = ctx.get('dask_client')
        return _persist_result(df, client)
    else:
        return df


def use_df(_, key, ctx=None, rename=None, columns=None, index=None, drop_index=True,
           ignore_null_error=False,
           empty_df_record=None,
           *args, **kwargs):
    datasets = ctx.datasets
    if key in datasets:
        df = datasets[key]
        if df is None:
            if ignore_null_error:
                return _empty_dd(empty_df_record)
            raise BlitzDatasetIsNull(u'use_df<%s> result is NULL' % key)
        if columns:
            df = df[columns]
        # print(df.columns)
        if index:
            if drop_index:
                if hasattr(df.index, 'nlevels') and df.index.nlevels > 1:
                    df = df.reset_index(drop_index).set_index(index)
            df = df.set_index(index)
        if rename:
            return df.rename(columns=rename)
        return to_dd(df)
    if ignore_null_error:
        return _empty_dd(empty_df_record)
    logger.warn(u'WARN: key<%s> not exists in datasets', key)
    raise BlitzDatasetIsNull(u'use_df invalid key<%s>' % key)


def df_groupby(df, by, ctx=None, ):
    # print(u'by===> %s' % by)
    # print(u'df_cols: %s' % df.columns)
    return df.groupby(by=by)


def df_count(df, column, rename=None, ctx=None, ):
    result = df[column].count()
    if rename:
        return result.rename(rename)
    else:
        return result


def df_max(df, column, rename=None, ctx=None, ):
    result = df[column].max()
    if rename:
        return result.rename(rename)
    else:
        return result


def df_min(df, column, rename=None, ctx=None, ):
    result = df[column].min()
    if rename:
        return result.rename(rename)
    else:
        return result


def df_mean(df, column, rename=None, ctx=None, ):
    result = df[column].mean()
    if rename:
        return result.rename(rename)
    else:
        return result


def df_median(df, column, rename=None, ctx=None, ):
    result = df[column].median()
    if rename:
        return result.rename(rename)
    else:
        return result


def df_agg(df, fn_list, ctx=None):
    return df.agg(fn_list)


def df_reset_index(df, rename=None, drop=False, ctx=None, *args, **kwargs):
    result = df.reset_index(drop=drop)
    if rename:
        return result.rename(columns=rename)
    return result


def stash_push_df(df, ctx=None, compute=True):
    if compute:
        client = ctx.get(u'dask_client')
        if client and is_dask_df(df):
            ctx.stash.append(df)
        else:
            ctx.stash.append(to_df(df))
    else:
        ctx.stash.append(df)
    return df


def stash_pop_df(_, ctx=None):
    return ctx.stash.pop()


def stash_clean(df, ctx=None):
    ctx.stash = []
    return df


def _force_rebuild_dask_df_with_client(df, client, chunk_size=10000, error_retries=2):
    if is_dask_df(df) and client:
        return to_dd(client.compute(df, sync=True, retries=error_retries), chunk_size)
    return df


def stash_join_df(_, on, how=u'outer', fillna=None, drop_stash=True, dtypes=None, ctx=None,
                  force_rebuild_dask=False, rebuid_chunk_size=10000,
                  dask_error_retries=2,
                  *args, **kwargs):
    stash = ctx.stash
    client = ctx.get(u'dask_client')
    result = stash.pop()
    if force_rebuild_dask:
        logger.warn(u'* !! force-rebuild-dask actived! this will spent more times.')
        result = _force_rebuild_dask_df_with_client(result, client, rebuid_chunk_size)
    # print(u'result, cols:%s' % _cat(result.columns))
    # print(u'    dtypes:%s' % result.dtypes)
    for other_df in stash:
        if force_rebuild_dask:
            logger.warn(u'* !! force-rebuild-dask actived! this will spent more times.')
            other_df = _force_rebuild_dask_df_with_client(other_df, client, rebuid_chunk_size,
                                                          dask_error_retries)
        result = result.merge(other_df, on=on, how=how, suffixes=(u'', u'_right'), )
    if fillna is not None:
        result = result.fillna(fillna)
    if dtypes:
        if is_dask_df(result):
            result = result.astype(dtypes)
        else:
            result = result.astype(dtypes, errors='ignore')
    if drop_stash:
        ctx.stash = []
    # print('===JOIN:%s' % result.columns)
    return to_dd(result, rebuid_chunk_size)


def stash_concat_df(_, ignore_index=True, drop_stash=True, dtypes=None, ctx=None,
                    force_rebuild_dask=False, rebuid_chunk_size=10000, *args,
                    **kwargs):
    stash = ctx.stash
    result = stash.pop()
    client = ctx.get(u'dask_client')
    if force_rebuild_dask:
        logger.warn(u'* !! force-rebuild-dask actived! this will spent more times.')
        result = _force_rebuild_dask_df_with_client(result, client, rebuid_chunk_size)
    for other_df in stash:
        if force_rebuild_dask:
            logger.warn(u'* !! force-rebuild-dask actived! this will spent more times.')
            other_df = _force_rebuild_dask_df_with_client(other_df, client, rebuid_chunk_size)
        if is_dask_df(result):
            result = result.append(other_df)
        else:
            result = result.append(other_df, ignore_index=ignore_index)
    if drop_stash:
        ctx.stash = []
    if dtypes:
        result = result.astype(dtypes, errors='ignore')
    return result


def df_sum_with_columns(df, src_columns, dest_column, init_value=0, ctx=None, *args, **kwargs):
    df[dest_column] = init_value
    cols = _ensure_cols(df, src_columns)
    if cols:
        for _ in cols:
            df[dest_column] = df[dest_column] + df[_]
    return df


def df_merge(df, other_df, on=None, how=u'left', left_index=False, right_index=False,
             force_rebuild_dask=False, chunk_size=5000,
             left_on=None, right_on=None,
             npartitions=None,
             persist=True, ctx=None, *args,
             **kwargs):
    if isinstance(other_df, dict):
        if u'key' not in other_df:
            raise CookSyntaxException(u'Syntax Error, missing key param, other-df<%s>' % other_df)
        other_df = use_df(None, ctx=ctx, **other_df)
    else:
        other_df = use_df(None, ctx=ctx, key=other_df)

    client = ctx.get(u'dask_client')
    if client and is_dask_df(df) and force_rebuild_dask:
        df = _force_rebuild_dask_df_with_client(df, client, chunk_size)
    if client and is_dask_df(other_df) and force_rebuild_dask:
        other_df = _force_rebuild_dask_df_with_client(other_df, client, chunk_size)
    merge_kwargs = dict(
        on=on, how=how, suffixes=(u'', u'_o'),
        left_on=left_on, right_on=right_on, left_index=left_index,
        right_index=right_index
    )
    if is_dask_df(df):
        merge_kwargs['npartitions'] = npartitions
    result = df.merge(other_df, **merge_kwargs)
    if persist and is_dask_df(result):
        return _persist_result(result, client)
    return result


def _persist_result(result, client=None):
    if is_dask_df(result):
        if client:
            return client.persist(result)
        return result.persist()
    return result


def df_sort_values(df, by, ascending=True, ctx=None, chunk_size=1000, *args, **kwargs):
    if is_dask_df(df):
        df = to_df(df)
    df = df.sort_values(by=by, ascending=ascending)
    return to_dd(df, chunksize=chunk_size)


def df_fillna(df, columns, value, ctx=None, *args, **kwargs):
    values = dict([(_, value) for _ in columns])
    return df.fillna(value=values)


def df_sum(df, column, rename=None, ctx=None, *args, **kwargs):
    result = df[column].sum()
    if rename:
        if is_list(column):
            return result.rename(columns=rename)
        return result.rename(rename)
    return result


def df_to_int(df, column, ctx=None, *args, **kwargs):
    if is_dask_df(df):
        # todo: optimize for daks-client mode
        df[column] = df[column].apply(to_int, meta=(column, 'int64'))
    else:
        df[column] = df[column].apply(to_int)
    return df


def df_to_float(df, column, ctx=None, *args, **kwargs):
    if is_dask_df(df):
        df[column] = df[column].apply(to_float, meta=(column, 'float64'))
    else:
        df[column] = df[column].apply(to_float)
    return df


def stack_push(df, ctx=None, *args, **kwargs):
    ctx.stack.append(df)
    return


def stack_pop(ctx=None, drop=False, *args, **kwargs):
    result = ctx.stack.pop()
    if drop:
        del result
        return
    return result


def stack_replace(df, ctx=None, *args, **kwargs):
    stack_pop(ctx=ctx, drop=True)
    stack_push(df, ctx=ctx)


def op_and(*args):
    s = []
    for _ in args:
        s.append(u'(%s)' % _)
    return u' & '.join(s)


def op_or(*args):
    s = []
    for _ in args:
        s.append(u'(%s)' % _)
    return u' | '.join(s)


def fn_range_table(df, column, range_table):
    """
        range_table =>
            ((0, 100), unit),
            ((0, 100), unit),
            ((100, ), unit)

    dsl syntax:

        - range_table:
            column: 完成单量
            table:
            - scope: [0, 100]
              unit: 5
            - scope: [100, 200]
              unit: 6
            - scope: [0,]
              unit: 7

    :param df:
    :type df:
    :param column:
    :type column:
    :param range_table:
    :type range_table:
    :return:
    :rtype:
    """
    pass


def load_dataset(_, key, ctx, dataset_cate=None, dataset_id=None, load_cache=True, ignore_empty=False, *args, **kwargs):
    dataset_ctx = ctx.datasets
    dataset_registry = ctx.dataset_registry
    dataset_dir = ctx.dataset_dir
    dataset_local_path = path_join(dataset_dir, u'%s.dataset' % key)
    if key in dataset_ctx:
        return dataset_ctx[key]

    if not dataset_cate:
        raise BlitzRuntimeException(u'dataset_cate is null')
    if dataset_cate == u'raw':
        loader = ctx.raw_dataset_loader
    elif dataset_cate == u'std':
        loader = ctx.std_dataset_loader
    elif dataset_cate == u'common':
        loader = ctx.dataset_loader
    else:
        raise BlitzRuntimeException(u'dataset_cate<%s> not support, valid value is raw|std|common')
    if not callable(loader):
        raise BlitzRuntimeException(u'context not setup loader<%s>' % dataset_cate)
    if not dataset_id:
        if not load_cache:
            raise BlitzRuntimeException(u'dataset_id is null')
        if not validate_dd_dataset_dir(dataset_local_path):
            raise BlitzRuntimeException(u'dataset_id is null and load_cache failed too.')
        df = dd.read_parquet(dataset_local_path, index=False)
    else:
        df = loader(dataset_id)
    if df is None:
        raise BlitzRuntimeException(u'raw_dataset<key:%s> not found.' % key)
    if df is not None and df_is_empty(df) and not ignore_empty:
        raise BlitzRuntimeException(u'raw_dataset<key:%s> is empty' % key)
    dd_to_parquet(df, dataset_local_path, compression=None, write_index=False)
    dataset_ctx[key] = df
    size = df_size(df)
    dataset_registry[key] = {
        u'path': dataset_local_path,
        u'dataset_id': dataset_id,
        u'dateset_cate': dataset_cate,
        u'time': prcnow().isoformat(),
        u'cnt': size,
    }
    return df


def store_dataset(df, key, ctx, dataset_cate=None, ):
    """Save current work data frame into dataset

    :param dataset_cate:
    :type dataset_cate:
    :param df:
    :type df:
    :param key:
    :type key:
    :param ctx:
    :type ctx:
    :return:
    :rtype:
    """
    dataset_ctx = ctx.datasets
    dataset_dir = ctx.dataset_dir
    dataset_registry = ctx.dataset_registry
    dataset_local_path = path_join(dataset_dir, u'%s.dataset' % key)
    dd_to_parquet(df, dataset_local_path, compression=None, write_index=False)
    dataset_ctx[key] = df
    size = df_size(df)
    dataset_registry[key] = {
        u'path': dataset_local_path,
        u'dateset_cate': dataset_cate or u'stash',
        u'time': prcnow().isoformat(),
        u'cnt': size,
    }
    return df


def push_dataset(df, key, ctx, *args, **kwargs):
    dataset_ctx = ctx.datasets
    dataset_ctx[key] = df
    return df


def pop_dataset(df, key, ctx, *args, **kwargs):
    dataset_ctx = ctx.datasets
    if key in dataset_ctx:
        return dataset_ctx.pop(key)
    return df


def df_pivot_table(df, values=None, index=None, columns=None, aggfunc='mean', ctx=None, *args, **kwargs):
    client = ctx.get(u'dask_client')
    if client and is_dask_df(df):
        df = client.compute(df, sync=True)
    else:
        df = to_df(df)
    df = df.pivot_table(index=index, columns=columns, values=values, aggfunc=aggfunc)
    df = to_dd(df, chunksize=10000)
    return df


def df_select_isna(df, column, ctx=None, *args, **kwargs):
    return df[df[column].isna()]


def df_select_notna(df, column, ctx=None, *args, **kwargs):
    return df[df[column].notna()]


def df_dropna(df, subset=None, axis=0, how='any', ctx=None, *args, **kwargs):
    return df.dropna(axis=axis, how=how, subset=subset)


def df_eval(df, exp_str, params=None, ctx=None, persist=False, *args, **kwargs):
    if not exp_str:
        return df
    column_tokens = _column_token_re.findall(exp_str)
    cn_cols = {}
    if column_tokens:
        for _ in column_tokens:
            c_name = _.strip(u'[]')
            if c_name not in cn_cols:
                cn_cols[c_name] = u'v_{}'.format(ObjectId())
            exp_str = exp_str.replace(_, cn_cols[c_name])
    if cn_cols:
        df = df.rename(columns=cn_cols)
    if params is None:
        params = {}
    df = df.eval(exp_str, local_dict=params, global_dict=ctx.global_vars)
    if cn_cols:
        if is_df_like(df):
            df = df.rename(columns=flip_dict(cn_cols))
    if persist:
        client = ctx.get('dask_client')
        return _persist_result(df, client)
    else:
        return df


def df_strip_space(df, columns, ctx=None, *args, **kwargs):
    return common_cook.strip_space_columns(df, columns)


def fetch_dataset(_, ctx, key=None, dataset_cate=None, columns=None, template_code=None, dataset_type_code=None,
                  index=False, sort=None, datakit_pull_way=u'*',
                  month_range=False,
                  month_offset=0,
                  month_delta=None,
                  min_month=None, max_month=None,
                  ignore_null_error=False,
                  empty_df_record=None,
                  month_value=None,
                  rename=None,
                  fallback_last=None,
                  *args, **kwargs):
    dataset_ctx = ctx.datasets
    if key and key in dataset_ctx:
        if rename:
            return dataset_ctx[key].rename(columns=rename)
        else:
            return dataset_ctx[key]
    if not dataset_cate:
        raise BlitzRuntimeException(u'dataset_cate is null')
    if month_value:
        if isinstance(month_value, basestring):
            month = ctx.get(month_value)
        else:
            month = int(month_value)
    else:
        month = ctx.month
    if month_offset:
        month = int(prc_from_today_int(u'%s01' % month).replace(months=month_offset).format(u'YYYYMM'))
    if dataset_cate == u'raw':
        dataset_fetcher = ctx.raw_dataset_fetcher
        fetcher_kwargs = dict(month=month, month_range=month_range, month_delta=month_delta, columns=columns,
                              min_month=min_month, max_month=max_month,
                              template_code=template_code,
                              source_pull_way=datakit_pull_way, index=index, sort=sort)
    elif dataset_cate == u'std':
        dataset_fetcher = ctx.std_dataset_fetcher
        fetcher_kwargs = dict(month=month, month_range=month_range, month_delta=month_delta, columns=columns,
                              min_month=min_month, max_month=max_month,
                              dataset_type_code=dataset_type_code, index=index,
                              sort=sort, fallback_last=fallback_last,
                              )
    else:
        raise BlitzRuntimeException(u'dataset_cate<%s> not support, valid value is raw|std|common' % dataset_cate)
    if not callable(dataset_fetcher):
        raise BlitzRuntimeException(u'context not setup loader<%s>' % dataset_cate)

    df = dataset_fetcher(**fetcher_kwargs)
    if df is None:
        if ignore_null_error:
            if empty_df_record:
                df = _empty_dd(empty_df_record)
            else:
                return
        else:
            raise BlitzDatasetIsNull(u'fetch_dataset args<%s> result is null.' % fetcher_kwargs)
    if rename:
        df = df.rename(columns=rename)
    if key:
        dataset_ctx[key] = df
    return df


def _is_list(v):
    return isinstance(v, list)


def _is_dict(v):
    return isinstance(v, dict)


def _empty_dd(record):
    return to_dd(pd.DataFrame(record if _is_list(record) else [record]))


def df_to_dask(_, chunksize=10000, ctx=None):
    return to_dd(_, chunksize=chunksize)


def df_to_pandas(df, ctx=None, *args, **kwargs):
    dask_client = ctx.dask_client
    if not is_dask_df(df):
        return df
    if dask_client:
        df = dask_client.compute(df, sync=True, retries=2)
    else:
        df = df.compute()
    return df


def df_set_index(df, keys, ctx=None, chunk_size=10000, *args, **kwargs):
    client = ctx.dask_client
    if is_dask_df(df) and isinstance(keys, list):
        logger.warn(
            u'dask Dataframe not support muti-level keys, fallback to pandas dataframe, this will hurt performance.')
        df = to_df(df)
        df = df.set_index(keys)
        return to_dd(df, chunk_size=chunk_size)
    return df.set_index(keys)


def dask_persist_compute(df, ctx=None, sync_compute=False, chunk_size=10000, *args, **kwargs):
    dask_client = ctx.dask_client
    if not is_dask_df(df):
        return df
    if dask_client:
        df = dask_client.persist(df)
        if sync_compute:
            df = dask_client.compute(df, sync=True, retries=2)
            return to_dd(df, chunksize=chunk_size)
    else:
        df = df.persist()
    return df


def dask_repartition(df, ctx=None, npartitions=10, force=True, *args, **kwargs):
    if not is_dask_df(df):
        return df
    df = df.repartition(npartitions=npartitions, force=force)
    return df


def df_nunique(df, column, rename=None, ctx=None, **kwargs):
    result = df[column].nunique()
    if rename:
        return result.rename(rename)
    else:
        return result


def df_rename_columns(df, rename, ctx=None, *args, **kwargs):
    return df.rename(columns=rename)


def df_isna(df, column, ctx=None, *args, **kwargs):
    return df[df[column].isna()]


def df_notna(df, column, ctx=None, *args, **kwargs):
    return df[df[column].notna()]


rename_cols = df_rename_columns


def df_rank(df, rank_cols, groupby=None, ascending=False, rank_suffix=u'_RANK', exclude_na=True, ctx=None,
            rank_method='min', pct=False, reindex=True,
            inf_as_na=True, na_option=u'keep',
            chunk_size=1000, *args,
            **kwargs):
    dask_client = ctx.dask_client
    if is_dask_df(df):
        if dask_client:
            df = dask_client.compute(df, sync=True, retries=2)
        else:
            df = to_df(df)
        if reindex:
            df = df.reset_index(drop=True).reindex()
    df_cols = df.columns
    exclude_values = [np.inf, -np.inf, np.nan]
    for col in rank_cols:
        if col not in df_cols:
            continue
        if exclude_na:
            df2 = df[~df[col].isin(exclude_values)]
        else:
            df2 = df
            if inf_as_na:
                df2[col] = df2[col].replace([np.inf, -np.inf], None)
        if groupby:
            df[u'{0}{1}'.format(col, rank_suffix)] = df2.groupby(groupby)[col].rank(
                method=rank_method, pct=pct, ascending=ascending, na_option=na_option)
        else:
            df[u'{0}{1}'.format(col, rank_suffix)] = df2[col].rank(
                method=rank_method,
                pct=pct,
                ascending=ascending,
                na_option=na_option
            )
    return to_dd(df, chunksize=chunk_size)


def run_py(df, code, ctx=None, force_df=False, *args, **kwargs):
    local_ctx = {
        'df': df,
        'ctx': ctx,
        'result': None,
    }
    global_ctx = globals()
    exec (code, global_ctx, local_ctx)
    if local_ctx[u'result'] is None:
        return df
    return local_ctx[u'result']


def df_tail(df, num=200, ctx=None, *args, **kwargs):
    return df.tail(num)


def df_head(df, num=200, ctx=None, *args, **kwargs):
    return df.head(num)


def df_sample(df, num=200, ctx=None, *args, **kwargs):
    return df.sample(num)


def df_set_column_val_if(df, column, condition, val, else_val=None, params=None, ctx=None, *args,
                         **kwargs):
    column_tokens = _column_token_re.findall(condition)
    cn_cols = {}
    if column_tokens:
        for _ in column_tokens:
            c_name = _.strip(u'[]')
            if c_name not in cn_cols:
                cn_cols[c_name] = u'v_{}'.format(ObjectId())
            condition = condition.replace(_, cn_cols[c_name])
    if cn_cols:
        df = df.rename(columns=cn_cols)
    if params is None:
        params = {}
    df_found = df.eval(condition, local_dict=params)
    if is_df_like(df_found):
        df_found_idx = df_found.index
    else:
        df_found_idx = df_found
    if cn_cols:
        if is_df_like(df):
            df = df.rename(columns=flip_dict(cn_cols))

    if else_val is not None:
        df[column] = else_val
    df[column] = df[column].mask(df_found_idx, val)
    return df


def df_py_select(df, code, ctx=None, *args, **kwargs):
    eval_ctx = {
        u'df': df,
        u'ctx': ctx,
    }
    idx = eval(code, globals(), eval_ctx)
    return df[idx]


def dask_apply_dtypes(df, dtypes, ctx=None, *args, **kwargs):
    if not is_dask_df(df):
        return df
    return df.astype(dtypes)


def set_global_vars(_, params, ctx=None, *args, **kwargs):
    ctx.global_vars.update(params)
    return _


def when_empty_fetch_dataset(_, ctx=None, **kwargs):
    if _ is not None and not df_is_empty(_):
        return _
    return fetch_dataset(_, ctx=ctx, **kwargs)


def when_empty_use_df(_, ctx=None, **kwargs):
    if _ is not None and not df_is_empty(_):
        return _
    return use_df(_, ctx=ctx, **kwargs)


def df_is_nan_inf(df, column, ctx=None, *args, **kwargs):
    condition = (np.isnan(df[column])) | (np.isinf(df[column]))
    return df[condition]


def df_not_nan_inf(df, column, ctx=None, *args, **kwargs):
    condition = (np.isnan(df[column])) | (np.isinf(df[column]))
    return df[~condition]


def df_drop_if_nan_inf(df, column, ctx=None, *args, **kwargs):
    condition = (np.isnan(df[column])) | (np.isinf(df[column]))
    df = to_df(df)
    df = df.drop(index=df[condition].index)
    return to_dd(df)


def df_round(df, columns, precision=4, ctx=None, *args, **kwargs):
    if not isinstance(columns, list):
        columns = [columns]
    for col in columns:
        if col not in columns:
            continue
        df[col] = df[col].round(precision)
    return df
