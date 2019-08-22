import psycopg2
import pandas as pd
import os


schema = 'performance'

################################################################################
# Inputs:
#  vault.parent_merchant_calendar_time_period
#  sandbox.extra_week_quarter_calendar
#       parent_merchant     label
#       abercrombie         4Q2016
#       abercrombie         4Q2017
# Output:
#  Removes 7 days::int from teh end_date of the quarter with extra week
################################################################################
prelim_extra_week_calendar_script = """
    with modified_calendar as (select a.parent_merchant, a.schedule, a.period_begin_date, a.period_end_date, a.label, a.series, a.days, a.report_days, a.report_end_date
        , a.report_days::int - a.days::int as days_left
        , dateadd(day, 7, a.period_begin_date)::date as begin_date_adjusted
        , dateadd(day, 7 , a.period_end_date)::date as end_date_adjusted
        , dateadd(day, 7, a.report_end_date)::date as report_end_date_adjusted
        , dateadd(day, 7 , a.period_end_date)::date as end_date_final
        from (
    select a.*
    from backtest_kpi.parent_merchant_calendar a
    join (select parent_merchant,'Prelim'||left(label,2) series_label from sandbox.extra_week_parent_merchant) b
    on a.parent_merchant = b.parent_merchant
    and a.series = b.series_label
    where a.days >=14) a

        order by parent_merchant, label )
    ,
    """

extra_week_calendar_script = """

    set wlm_query_slot_count to 4;
    set query_group to 'long' ;
    with modified_calendar as (select a.parent_merchant, a.schedule, a.period_begin_date, a.period_end_date, a.label, a.series, a.days, a.report_days, a.report_end_date
    , a.report_days::int - a.days::int as days_left
    , dateadd(day, 0 , a.period_begin_date)::date as begin_date_adjusted
    , dateadd(day, -7 , a.period_end_date)::date as end_date_adjusted
    , dateadd(day, -7 , a.report_end_date)::date as report_end_date_adjusted
    , dateadd(day, -7 , a.period_end_date)::date as end_date_final
    from backtest_kpi.parent_merchant_calendar a
    join sandbox.extra_week_quarter_calendar b
    on a.parent_merchant = b.parent_merchant
    and a.label = b.label
    where a.days::int >= 14
    and a.parent_merchant in (
    select parent_merchant from sandbox.extra_week_parent_merchant
    )
    order by parent_merchant, label )
    ,
    """
ratio_extra_week_calendar_script = """

    set wlm_query_slot_count to 4;
    set query_group to 'long' ;
    with modified_calendar as (select a.parent_merchant, a.schedule, a.period_begin_date, a.period_end_date, a.label, a.series, a.days, a.report_days, a.report_end_date
    , a.report_days::int - a.days::int as days_left
    , dateadd(day, 7 , a.period_begin_date)::date as begin_date_adjusted
    , dateadd(day, 0 , a.period_end_date)::date as end_date_adjusted
    , dateadd(day, 0 , a.report_end_date)::date as report_end_date_adjusted
    , dateadd(day, 0 , a.period_end_date)::date as end_date_final
    from backtest_kpi.parent_merchant_calendar a
    join sandbox.extra_week_quarter_calendar b
    on a.parent_merchant = b.parent_merchant
    and a.label = b.label
    where a.days::int >= 14
    and a.parent_merchant in (
    select parent_merchant from sandbox.extra_week_parent_merchant
    )
    order by parent_merchant, label )
    ,
    """

lapping_extra_week_calendar_script = """

    set wlm_query_slot_count to 4;
    set query_group to 'long' ;
    with modified_calendar as (select a.parent_merchant, a.schedule, a.period_begin_date, a.period_end_date, a.label, a.series, a.days, a.report_days, a.report_end_date
    , a.report_days::int - a.days::int as days_left
    , dateadd(day, 7 , a.period_begin_date)::date as begin_date_adjusted
    , dateadd(day, 0 , a.period_end_date)::date as end_date_adjusted
    , dateadd(day, 0 , a.report_end_date)::date as report_end_date_adjusted
    , dateadd(day, 0 , a.period_end_date)::date as end_date_final
    from backtest_kpi.parent_merchant_calendar  a
    join sandbox.calendar_shift_quarter_calendar b
    on a.parent_merchant = b.parent_merchant
    and a.label = b.label
    where a.days::int >= 14
    and a.parent_merchant in (
    select parent_merchant from sandbox.extra_week_parent_merchant
    )
    order by parent_merchant, label )
    ,
    """
################################################################################
# Inputs:
#  vault.parent_merchant_calendar_time_period
#  sandbox.calendar_shift_quarter_calendar
#       parent_merchant     label
#       abercrombie         Prelim1Q2017
#       abercrombie         Prelim1Q2018
# Output:
#  Adds shifts begin and end date by 7 days
################################################################################
calendar_shift_calendar_script = """

    set wlm_query_slot_count to 4;
    set query_group to 'long' ;
    with modified_calendar as (select a.parent_merchant, a.schedule, a.period_begin_date, a.period_end_date, a.label, a.series, a.days, a.report_days, a.report_end_date
    , a.report_days::int - a.days::int as days_left
    , dateadd(day, 7 , a.period_begin_date)::date as begin_date_adjusted
    , dateadd(day, 7 , a.period_end_date)::date as end_date_adjusted
    , dateadd(day, 7 , a.report_end_date)::date as report_end_date_adjusted
    , dateadd(day, 7 , a.period_end_date)::date as end_date_final
    from backtest_kpi.parent_merchant_calendar  a
    join sandbox.calendar_shift_quarter_calendar b
    on a.parent_merchant = b.parent_merchant
    and a.label = b.label
    where a.days::int >= 14
    and a.parent_merchant in (
    select parent_merchant from sandbox.extra_week_parent_merchant
    )
    order by parent_merchant, label )
    ,
    """

def cs_method_script(table_name, days = ' '):
    script = """
    current_panel_sales as (
    select y.parent_merchant, y.merchant, series
    , e.label,days, report_days, days_left
    , period_begin_date, period_end_date
    , sum(cs_amount_all::numeric(20,10)) as transaction_amount_current
    from sandbox.machine_data_daily_jzhou y
    join modified_calendar e
    on y.parent_merchant = e.parent_merchant
    where y.date between e.period_begin_date ANd e.period_end_date
    and y.merchant != 'all'
    group by y.parent_merchant, y.merchant, series, e.label, days, report_days, days_left, period_begin_date, period_end_date
    order by y.parent_merchant, y.merchant, series, e.label, days, report_days, days_left
    )
    ,
    previous_panel_sales as (
    select y.parent_merchant, y.merchant, series, e.label,days, report_days, days_left,begin_date_adjusted ,end_date_final
    , sum(cs_amount_all::numeric(20,10)) as transaction_amount_previous
    from sandbox.machine_data_daily_jzhou y
    join modified_calendar e
    on y.parent_merchant = e.parent_merchant
    where y.date between e.begin_date_adjusted AND e.end_date_final
    and y.merchant != 'all'
    group by y.parent_merchant, y.merchant, series, e.label, days, report_days, days_left,begin_date_adjusted, end_date_final
    order by y.parent_merchant, y.merchant, series, e.label, days, report_days, days_left
    ),
    -- unadjusted
    current_panel_sales_parent as (
    select y.parent_merchant, series, e.label,days, report_days, days_left,period_begin_date, period_end_date,sum(cs_amount_all::numeric(20,10)) as transaction_amount_current
    from sandbox.machine_data_daily_jzhou y
    join modified_calendar e
    on y.parent_merchant = e.parent_merchant
    where y.date between e.period_begin_date ANd e.period_end_date
    and y.merchant = 'all'
    group by y.parent_merchant, series, e.label, days, report_days, days_left, period_begin_date, period_end_date
    order by y.parent_merchant, series, e.label, days, report_days, days_left
    )
    ,
    previous_panel_sales_parent as (
    select y.parent_merchant, series, e.label,days, report_days, days_left, begin_date_adjusted ,end_date_final, sum(cs_amount_all::numeric(20,10)) as transaction_amount_previous
    from sandbox.machine_data_daily_jzhou y
    join modified_calendar e
    on y.parent_merchant = e.parent_merchant
    where y.date between e.begin_date_adjusted AND e.end_date_final
    and y.merchant = 'all'
    group by y.parent_merchant, series, e.label, days, report_days, days_left,begin_date_adjusted, end_date_final
    order by y.parent_merchant, series, e.label, days, report_days, days_left
    )
    ,
    data_table as (
    select a.*, b.transaction_amount_previous,  b.begin_date_adjusted, b.end_date_final
    , row_number() over (partition by a.parent_merchant, a.merchant order by b.label) as rows
    from current_panel_sales a
    inner join previous_panel_sales b
    on a.parent_merchant = b.parent_merchant
    and a.merchant = b.merchant
    and a.label = b.label
    )
    ,
    data_table_parent as (
    select a.*, b.transaction_amount_previous,  b.begin_date_adjusted, b.end_date_final
    , row_number() over (partition by a.parent_merchant order by b.label) as rows
    from current_panel_sales_parent a
    inner join previous_panel_sales_parent b
    on a.parent_merchant = b.parent_merchant
    and a.label = b.label

    )
    ,
    data_table_merchant_final as (
    select a.parent_merchant
    , a.merchant
    , a.days
    , a.label as previous_label
    , a.period_begin_date as current_begin_date
    , a.period_end_date as current_end_date
    , a.begin_date_adjusted as current_begin_date_adjusted
    , a.end_date_final as current_end_date_final
    , b.label as current_label
    , a.transaction_amount_current as previous_transaction_amount
    , a.transaction_amount_previous as previous_transaction_amount_adjusted
    , b.transaction_amount_current as current_transaction_amount
    , b.transaction_amount_previous as current_transaction_amount_adjusted
    from data_table a
    inner join data_table b
    on a.parent_merchant = b.parent_merchant
    and a.merchant = b.merchant
    and left(b.label, 1) = left(a.label,1)
    and right(b.label, 1)::numeric = (right(a.label, 1)::numeric +1)
    order by a.parent_merchant, a.merchant, a.label
    )
    ,
    data_table_parent_merchant_final as (
    select a.parent_merchant
    , 'all'::varchar(255) as merchant
    , a.days
    , a.label as previous_label
    , a.period_begin_date as current_begin_date
    , a.period_end_date as current_end_date
    , a.begin_date_adjusted as current_begin_date_adjusted
    , a.end_date_final as current_end_date_final
    , b.label as current_label
    , a.transaction_amount_current as previous_transaction_amount
    , a.transaction_amount_previous as previous_transaction_amount_adjusted
    , b.transaction_amount_current as current_transaction_amount
    , b.transaction_amount_previous as current_transaction_amount_adjusted
    from data_table_parent a
    inner join data_table_parent b
    on a.parent_merchant = b.parent_merchant
    and left(b.label, 1) = left(a.label,1)
    and right(b.label, 1)::numeric = (right(a.label, 1)::numeric +1)
    order by a.parent_merchant, a.label
    ),
    combined_table as (
    select * from data_table_parent_merchant_final
    union all
    select * from data_table_merchant_final
    )
    select
    b.order_id::int
    , b.format_name
    , a.*
    , 'cs' as method_flag
    into sandbox.{table_name}
    from combined_table a
    inner join backtest_kpi.excel_options b
    on replace(a.parent_merchant,'_adjusted', '') = replace(b.parent_merchant,'_adjusted', '')
    and replace(a.merchant,'_adjusted', '') = replace(b.merchant,'_adjusted', '')
     {days}
    order by order_id
    """.format(table_name = table_name, days = days)
    return script

def index_method_script(table_name, days = ' '):
    script = """
    current_panel_sales as (
    select y.parent_merchant, y.merchant, series, e.label,days, report_days
    , days_left,period_begin_date, period_end_date
    , sum(index_blended_amount_all::numeric(20,10)) as transaction_amount_current
    from sandbox.machine_data_daily_jzhou y
    join modified_calendar e
    on y.parent_merchant = e.parent_merchant
    where y.date between e.period_begin_date ANd e.period_end_date
    -- and source = 'card'
    and y.merchant != 'all'
    group by y.parent_merchant, y.merchant, series, e.label, days, report_days, days_left, period_begin_date, period_end_date
    order by y.parent_merchant, y.merchant, series, e.label, days, report_days, days_left
    )
    ,
    previous_panel_sales as (
    select y.parent_merchant, y.merchant, series, e.label,days, report_days
    , days_left,begin_date_adjusted ,end_date_final,
     sum(index_blended_amount_all::numeric(20,10)) as transaction_amount_previous
    from sandbox.machine_data_daily_jzhou y
    join modified_calendar e
    on y.parent_merchant = e.parent_merchant
    where y.date between e.begin_date_adjusted AND e.end_date_final
    -- and source = 'card'
    and y.merchant != 'all'
    group by y.parent_merchant, y.merchant, series, e.label, days, report_days, days_left,begin_date_adjusted, end_date_final
    order by y.parent_merchant, y.merchant, series, e.label, days, report_days, days_left
    ),


    current_panel_sales_parent as (
    select y.parent_merchant, series, e.label,days
    , report_days, days_left, period_begin_date, period_end_date
    , sum(index_blended_amount_all::numeric(20,10)) as transaction_amount_current
    from sandbox.machine_data_daily_jzhou y
    join modified_calendar e
    on y.parent_merchant = e.parent_merchant

    where y.date between e.period_begin_date ANd e.period_end_date
    -- and source = 'card'
    and y.merchant = 'all'
    group by y.parent_merchant, series, e.label, days, report_days, days_left, period_begin_date, period_end_date
    order by y.parent_merchant, series, e.label, days, report_days, days_left
    )
    ,

    previous_panel_sales_parent as (
    select y.parent_merchant, series, e.label,days, report_days
    , days_left,begin_date_adjusted ,end_date_final
    , sum(index_blended_amount_all::numeric(20,10)) as transaction_amount_previous
    from sandbox.machine_data_daily_jzhou y
    join modified_calendar e
    on y.parent_merchant = e.parent_merchant

    where y.date between e.begin_date_adjusted AND e.end_date_final
    -- and source = 'card'
    and y.merchant = 'all'
    group by y.parent_merchant, series, e.label, days, report_days, days_left,begin_date_adjusted, end_date_final
    order by y.parent_merchant, series, e.label, days, report_days, days_left

    )
    ,
    data_table as (
    select a.*, b.transaction_amount_previous,  b.begin_date_adjusted, b.end_date_final

    , row_number() over (partition by a.parent_merchant, a.merchant order by b.label) as rows
    from current_panel_sales a
    inner join previous_panel_sales b
    on a.parent_merchant = b.parent_merchant
    and a.merchant = b.merchant
    and a.label = b.label

    )
    ,
    data_table_parent as (
    select a.*, b.transaction_amount_previous,  b.begin_date_adjusted, b.end_date_final

    , row_number() over (partition by a.parent_merchant order by b.label) as rows
    from current_panel_sales_parent a
    inner join previous_panel_sales_parent b
    on a.parent_merchant = b.parent_merchant
    and a.label = b.label

    )

    ,
    data_table_merchant_final as (
    select a.parent_merchant
    , a.merchant
    , a.days
    , a.label as previous_label
    , a.period_begin_date as current_begin_date
    , a.period_end_date as current_end_date
    , a.begin_date_adjusted as current_begin_date_adjusted
    , a.end_date_final as current_end_date_final
    , b.label as current_label
    , a.transaction_amount_current as previous_transaction_amount
    , a.transaction_amount_previous as previous_transaction_amount_adjusted

    , b.transaction_amount_current as current_transaction_amount
    , b.transaction_amount_previous as current_transaction_amount_adjusted

    from data_table a
    inner join data_table b
    on a.parent_merchant = b.parent_merchant
    and a.merchant = b.merchant
    and left(b.label, 1) = left(a.label,1)
    and right(b.label, 1)::numeric = (right(a.label, 1)::numeric +1)
    order by a.parent_merchant, a.merchant, a.label
    )

    ,

    data_table_parent_merchant_final as (
    select a.parent_merchant
    , 'all'::varchar(255) as merchant
    , a.days
    , a.label as previous_label
    , a.period_begin_date as current_begin_date
    , a.period_end_date as current_end_date
    , a.begin_date_adjusted as current_begin_date_adjusted
    , a.end_date_final as current_end_date_final
    , b.label as current_label
    , a.transaction_amount_current as previous_transaction_amount
    , a.transaction_amount_previous as previous_transaction_amount_adjusted

    , b.transaction_amount_current as current_transaction_amount
    , b.transaction_amount_previous as current_transaction_amount_adjusted

    from data_table_parent a
    inner join data_table_parent b
    on a.parent_merchant = b.parent_merchant
    and left(b.label, 1) = left(a.label,1)
    and right(b.label, 1)::numeric = (right(a.label, 1)::numeric +1)
    order by a.parent_merchant, a.label
    )
    ,

    combined_table as (
    select * from data_table_parent_merchant_final
    union
    select * from data_table_merchant_final
    )
    ,

    parent_merchant_denom_unadjusted as (
    select e.parent_merchant
    , e.label, e.days,sum(s.transactions::numeric(20,10)) as unadjusted_denom
    , row_number() over (partition by parent_merchant order by label)as rows
    from backtest_kpi.daily_denom s
    join modified_calendar e
    on s.optimized_date between e.period_begin_date ANd e.period_end_date
    where s.blended_denom_filter is Null
    and source is null
    and cobrand_id in (10005640, 10006164)
    group by e.parent_merchant, e.label,e.days
    order by e.parent_merchant, e.label,e.days
    )

    ,
    parent_merchant_denom_adjusted as (
    select e.parent_merchant ,e.label,e.days, sum(s.transactions::numeric(20,10)) as adjusted_denom
    , row_number() over (partition by parent_merchant order by label)as rows
    from backtest_kpi.daily_denom s
    join modified_calendar e
    on s.optimized_date between e.begin_date_adjusted ANd e.end_date_final
    where s.blended_denom_filter is Null
    and source is null
    and cobrand_id in (10005640, 10006164)
    group by e.parent_merchant, e.label,e.days
    order by e.parent_merchant, e.label,e.days
    )

    ,
    denom_table as (
    select c.parent_merchant
    , c.label
    , c.unadjusted_denom
    , d.adjusted_denom
    , c.rows
    , c.days::int as days
    from parent_merchant_denom_unadjusted c

    inner join parent_merchant_denom_adjusted d
    on replace(c.parent_merchant,'_adjusted', '') = replace(d.parent_merchant,'_adjusted', '')
    and c.label = d.label
    and c.rows = d.rows
    -- where a.parent_merchant = 'gap'
    )

    ,
    denom_table_final as ( select
    c.parent_merchant
    , c.label as current_label
    , d.label as previous_label
    , d.unadjusted_denom as previous_denom_unadjusted
    , d.adjusted_denom as previous_denom_adjusted
    , c.unadjusted_denom as current_denom_unadjusted
    , c.adjusted_denom as current_denom_adjusted
    , c.days::int as current_days
    , d.days::int as previous_days
    from denom_table c
    inner join denom_table d
    on replace(c.parent_merchant,'_adjusted', '') = replace(d.parent_merchant,'_adjusted', '')
    and left(c.label, 1) = left(d.label,1)
    and right(c.label, 2)::numeric = (right(d.label, 2)::numeric +1)
    )

    select
      b.order_id::int
    , b.format_name
    , a.*
    , c.previous_denom_unadjusted
    , c.previous_denom_adjusted
    , c.current_denom_unadjusted
    , c.current_denom_adjusted
    , c.previous_days
    , c.current_days
    , 'index' as method_flag
    into sandbox.{table_name}
    from combined_table a
    inner join backtest_kpi.excel_options b
    on replace(a.parent_merchant,'_adjusted', '') = replace(b.parent_merchant,'_adjusted', '')
    and replace(a.merchant,'_adjusted', '') = replace(b.merchant,'_adjusted', '')
    inner join denom_table_final c
    on replace(a.parent_merchant,'_adjusted', '') = replace(c.parent_merchant,'_adjusted', '')
    and a.current_label = c.current_label
    and a.previous_label = c.previous_label
     {days}
    order by order_id
    """.format(table_name = table_name, days = days)
    return(script)

#
# def sql_execute(sql_query):
#     con = pool.getconn()
#     con.autocommit = True
#     cur = con.cursor()
#     cur.execute(sql_query)
#     col_names = []
#     try:
#         for elt in cur.description:
#             col_names.append(elt[0])
#         df_query = pd.DataFrame(cur.fetchall(), columns = col_names)
#         cur.close()
#         con.close()
#         return(df_query)
#     except TypeError:
#         cur.close()
#         con.close()
#         return
#     return

################################################################################
# Executes sql-script. Returns output as dataframe is specified
# sql_query: SQL code to run. Can be in multiple statements.
# con: Connection to use. If not specified then defaults to performance
# has_return: Whether we are expecting to return a dataframe
# Return: Null or dataframe
################################################################################

def sql_execute(sql_query, con = None, has_return = False):

    if con is None:
        con_active = psycopg2.connect(
            dbname = os.environ.get("PGDATABASE"),
            host = os.environ.get("PGHOST"),
            port = os.environ.get("PGPORT"),
            user = os.environ.get("PGUSER"),
            password = os.environ.get("PGPASSWORD")
        )
    else:
        con_active = con
    con_active.autocommit = True
    cur = con_active.cursor()
    cur.execute(sql_query)
    if has_return:
        col_names = []
        for elt in cur.description:
        	col_names.append(elt[0])
        df_penetration = pd.DataFrame(cur.fetchall(), columns = col_names)
        cur.close()
        if con is None:
            con_active.close()
        return(df_penetration)
    else:
        cur.close()
        return


def index_extra_week_function(input_string1, input_string2):
    index_extra_week_string = input_string1 + input_string2
    # print(index_extra_week_tring)
    index_extra_week = sql_execute(index_extra_week_string)
    # index_extra_week.to_csv("/Users/Jack Zhou/Documents/Python Scripts/Adjusted Panel Sales/1_index_extra_week_{}.csv".format(schema), mode = 'w',sep = '|', header = True, index = False)
    return

def cs_extra_week_function(input_string1, input_string2):
    cs_extra_week_string =  input_string1 + input_string2
    # print(cs_extra_week_string)
    cs_extra_week = sql_execute(cs_extra_week_string)
    # cs_extra_week.to_csv("/Users/Jack Zhou/Documents/Python Scripts/Adjusted Panel Sales/3_cs_extra_week_{}.csv".format(schema), mode = 'w',sep = '|', header = True, index = False)
    return

def cs_extra_week_function_channel(input_string1, input_string2):
    cs_extra_week_string =  input_string1 + input_string2
    cs_extra_week = sql_execute(cs_extra_week_string)
    # cs_extra_week.to_csv("/Users/Jack Zhou/Documents/Python Scripts/Adjusted Panel Sales/3_cs_extra_week_{}.csv".format(schema), mode = 'w',sep = '|', header = True, index = False)
    return

def index_calendar_shift(input_string1, input_string2):
    index_current_string = input_string1 + input_string2
    index_current = sql_execute(index_current_string)
    # index_current.to_csv("/Users/Jack Zhou/Documents/Python Scripts/Adjusted Panel Sales/2_index_current_{}.csv".format(schema), mode = 'w',sep = '|', header = True, index = False)
    return

def cs_calendar_shift(input_string1, input_string2):
    cs_current_string = input_string1 + input_string2
    # print(cs_current_string)
    cs_current = sql_execute(cs_current_string)
    # cs_current.to_csv("/Users/Jack Zhou/Documents/Python Scripts/Adjusted Panel Sales/4_cs_current_{}.csv".format(schema), mode = 'w',sep = '|', header = True, index = False)
    return

def cs_calendar_shift_channel(input_string1, input_string2):
    cs_current_string = input_string1 + input_string2
    cs_current = sql_execute(cs_current_string)
    # cs_current.to_csv("/Users/Jack Zhou/Documents/Python Scripts/Adjusted Panel Sales/4_cs_current_{}.csv".format(schema), mode = 'w',sep = '|', header = True, index = False)
    return
####### Start Script ########



extra_week_parent_merchant_string  = """
    drop table if exists sandbox.extra_week_parent_merchant;
    with calendar as (select *,
    row_number() over (partition by parent_merchant order by period_end_date desc, label desc ) as rows
    from backtest_kpi.parent_merchant_calendar
    where label not like '%Prelim%'
    )
    ,
    temp_calendar as (
     select *, report_days::int - lead(report_days, 4) over (partition by parent_merchant order by rows) as lag_days from calendar
    where rows <= 9
order by rows desc)


    select parent_merchant, label into sandbox.extra_week_parent_merchant from temp_calendar a
    where lag_days = 7
    and a.parent_merchant in (select distinct b.parent_merchant from backtest_kpi.backtest_options b)


    order by parent_merchant;

    """
extra_week_parent_merchant = sql_execute(extra_week_parent_merchant_string)
# print('Extra Week Identification Complete. It took ' + str(time.time() - current_time) + ' seconds')

extra_week_quarter_calendar_string = """
    drop table if exists sandbox.extra_week_quarter_calendar;
    select a.parent_merchant, left(a.label, length(a.label) -2 ) || (right(a.label,2)::numeric - 1) as label into sandbox.extra_week_quarter_calendar from sandbox.extra_week_parent_merchant a
    union all
    select a.parent_merchant, a.label from sandbox.extra_week_parent_merchant a ;

    drop table if exists sandbox.calendar_shift_quarter_calendar;

    with core_dictionary as (
    select a.parent_merchant, a.label, b.report_end_date as test_date, b.period_end_date  from sandbox.extra_week_parent_merchant a
    join backtest_kpi.parent_merchant_calendar b
    on a.parent_merchant = b.parent_merchant
    and a.label = b.label )
    ,
    quarters as (
    select b.parent_merchant, b.label from core_dictionary a
    join backtest_kpi.parent_merchant_calendar b
    on b.report_start_date > a.test_date
    and b.report_start_date < dateadd(day, 91*4 - 4, a.test_date)
    and a.parent_merchant = b.parent_merchant
    and b.days::int >= 14
    where b.parent_merchant||left(b.label, length(b.label) -2 ) || (right(b.label,2)::numeric - 1) in (select distinct parent_merchant||label from backtest_kpi.parent_merchant_calendar )
    )
    select a.parent_merchant, left(a.label, length(a.label) -2 ) || (right(a.label,2)::numeric - 1) as label into sandbox.calendar_shift_quarter_calendar from quarters a
    union all
    select a.parent_merchant, a.label from quarters a ;

    ;"""
# print('Extra Week Calendar Generation Starting')

extra_week_quarter_calendar = sql_execute(extra_week_quarter_calendar_string)
# print('Extra Week Calendar Generation Complete. It took ' + str(time.time() - current_time) + ' seconds')

machine_data_daily_jzhou = """
    set wlm_query_slot_count to 4;
    set query_group to 'long' ;
    drop table if exists sandbox.machine_data_daily_jzhou;
    select replace(b.parent_merchant, '_adjusted','') parent_merchant, replace(b.merchant,'_adjusted','') merchant, a.* into sandbox.machine_data_daily_jzhou from backtest_kpi.machine_data_daily a
    inner join backtest_kpi.excel_options b
    on a.tab = b.tab
    where replace(b.parent_merchant, '_adjusted','') in (select distinct parent_merchant from sandbox.extra_week_parent_merchant)
;
"""

sql_execute(machine_data_daily_jzhou)
# print('Machine Data Daily Gen Complete. It took ' + str(time.time() - current_time) + ' seconds')

new_table_script = """
    drop table if exists sandbox.jzhou_7_blend_panel_sales;
    select a.order_id, a.format_name, a.current_label, a.previous_transaction_amount_adjusted pv_previous_adjusted
    ,a.current_transaction_amount pv_current_adjusted
    ,b.previous_transaction_amount store_sales_previous_unadjusted
    ,b.current_transaction_amount store_sales_current_unadjusted

    , a.previous_transaction_amount_adjusted + b.previous_transaction_amount as panel_sales_prior_blended
    , a.current_transaction_amount + b.current_transaction_amount as panel_sales_current_blended


    , a.previous_transaction_amount + b.previous_transaction_amount as panel_sales_prior
    , a.current_transaction_amount + b.current_transaction_amount as panel_sales_current
    into sandbox.jzhou_7_blend_panel_sales
    from sandbox.jzhou_5_cs_calendar_shift_payment_volume a
    join sandbox.jzhou_6_cs_calendar_shift_store_sales b
    on a.format_name = b.format_name
    and a.current_label = b.current_label
    where a.format_name in (select format_name from backtest_kpi.excel_options
    where format = 'captive')

    order by order_id

    """

drop_table_script = """
                    drop table if exists sandbox.jzhou_1_index_extra_week CASCADE;
                    drop table if exists sandbox.jzhou_2_cs_extra_week CASCADE;
                    drop table if exists sandbox.jzhou_3_index_calendar_shift CASCADE;
                    drop table if exists sandbox.jzhou_4_cs_calendar_shift CASCADE;
                    drop table if exists sandbox.jzhou_5_index_prelim_extra_week CASCADE;
                    drop table if exists sandbox.jzhou_6_cs_prelim_extra_week CASCADE;
                    drop table if exists sandbox.jzhou_5_index_ratio CASCADE;
                    drop table if exists sandbox.jzhou_6_cs_ratio CASCADE;
                    drop table if exists sandbox.jzhou_3_1_index_calendar_shift CASCADE;
                    drop table if exists sandbox.jzhou_4_1_cs_calendar_shift CASCADE;
                    """
# print("Running Drop table script")
sql_execute(drop_table_script)

excluded_quarter_script = """
        where a.parent_merchant||a.current_label in (select distinct parent_merchant||left(quarter,2)||'20'||(right(quarter,2)-1)::int
        from backtest_kpi.quarter_reported)

    """




index_extra_week_function(extra_week_calendar_script, index_method_script('jzhou_1_index_extra_week', days = ''))
cs_extra_week_function(extra_week_calendar_script, cs_method_script('jzhou_2_cs_extra_week', days = ''))
index_calendar_shift(calendar_shift_calendar_script, index_method_script('jzhou_3_index_calendar_shift', days = ' where a.days != 98 '))
cs_calendar_shift(calendar_shift_calendar_script, cs_method_script('jzhou_4_cs_calendar_shift', days = ' where a.days != 98 '))
index_extra_week_function(ratio_extra_week_calendar_script, index_method_script('jzhou_5_index_ratio', days = excluded_quarter_script))
cs_extra_week_function(ratio_extra_week_calendar_script, cs_method_script('jzhou_6_cs_ratio', days = excluded_quarter_script))
index_extra_week_function(prelim_extra_week_calendar_script,index_method_script('jzhou_5_index_prelim_extra_week', days = excluded_quarter_script))
cs_extra_week_function(prelim_extra_week_calendar_script,cs_method_script('jzhou_6_cs_prelim_extra_week', days = excluded_quarter_script))


index_calendar_shift(lapping_extra_week_calendar_script, index_method_script(table_name = 'jzhou_3_1_index_calendar_shift', days = ' where a.days = 98 '))
cs_calendar_shift(lapping_extra_week_calendar_script, cs_method_script(table_name = 'jzhou_4_1_cs_calendar_shift', days = ' where a.days = 98 '))



extra_week_prelim_script ="""
    drop table if exists sandbox.jzhou_5_index_prelim_extra_week_final;
    select a.*, b.current_transaction_amount/b.current_transaction_amount_adjusted as transaction_amount_ratio
        , b.current_denom_unadjusted/b.current_denom_adjusted as denom_ratio
        , b.current_days/b.previous_days::decimal as day_ratio
    into sandbox.jzhou_5_index_prelim_extra_week_final
    from sandbox.jzhou_5_index_prelim_extra_week a
    join sandbox.jzhou_5_index_ratio b
        on a.format_name = b.format_name
        and a.previous_label = 'Prelim'||b.current_label
    where b.current_transaction_amount_adjusted > 0
        and b.current_denom_adjusted > 0
    ;
    drop table if exists sandbox.jzhou_6_cs_prelim_extra_week_final;

    select a.*, b.current_transaction_amount/b.current_transaction_amount_adjusted as transaction_amount_ratio
    into sandbox.jzhou_6_cs_prelim_extra_week_final
    from sandbox.jzhou_6_cs_prelim_extra_week a
    join sandbox.jzhou_6_cs_ratio b
        on a.format_name = b.format_name
        and a.previous_label = 'Prelim'||b.current_label
    where b.current_transaction_amount_adjusted > 0
        ;


    """
sql_execute(extra_week_prelim_script)



temp_script = """
    INSERT into sandbox.jzhou_3_index_calendar_shift
    (select * from sandbox.jzhou_3_1_index_calendar_shift
    where parent_merchant||current_label in
    (select distinct parent_merchant||left(label,2)||'20'||(right(label,2)::int+1) from sandbox.extra_week_parent_merchant)
    );


    INSERT into sandbox.jzhou_4_cs_calendar_shift
    (select * from sandbox.jzhou_4_1_cs_calendar_shift
    where parent_merchant||current_label in
    (select distinct parent_merchant||left(label,2)||'20'||(right(label,2)::int+1) from sandbox.extra_week_parent_merchant)
    );
"""
sql_execute(temp_script)

temp_script = """
    DELETE from sandbox.jzhou_3_index_calendar_shift
    where parent_merchant in (select distinct parent_merchant from sandbox.jzhou_3_1_index_calendar_shift)
    and current_label = 'Prelim1Q2019'
    ;
    DELETE from sandbox.jzhou_3_index_calendar_shift
    where parent_merchant = 'red_robin'
    and current_label = 'Prelim1Q2019'
    ;
    DELETE from sandbox.jzhou_4_cs_calendar_shift
    where parent_merchant in (select distinct parent_merchant from sandbox.jzhou_4_1_cs_calendar_shift)
    and current_label = 'Prelim1Q2019'
    ;
    DELETE from sandbox.jzhou_4_cs_calendar_shift
    where parent_merchant = 'red_robin'
    and current_label = 'Prelim1Q2019'
    ;
    """
sql_execute(temp_script)

access_script = """
                    grant all on sandbox.machine_data_daily_jzhou to dwhanalyst;
                    grant all on sandbox.jzhou_1_index_extra_week to dwhanalyst;
                    grant all on sandbox.jzhou_2_cs_extra_week to dwhanalyst;
                    grant all on sandbox.jzhou_3_index_calendar_shift to dwhanalyst;
                    GRANT ALL ON sandbox.jzhou_4_cs_calendar_shift to dwhanalyst;
                    GRANT ALL ON sandbox.jzhou_5_index_prelim_extra_week to dwhanalyst;
                    GRANT ALL ON sandbox.jzhou_6_cs_prelim_extra_week to dwhanalyst;
                    GRANT ALL ON sandbox.jzhou_5_index_ratio to dwhanalyst;
                    GRANT ALL ON sandbox.jzhou_6_cs_ratio to dwhanalyst;
                    GRANT ALL ON sandbox.jzhou_5_index_prelim_extra_week_final to dwhanalyst;
                    GRANT ALL ON sandbox.jzhou_6_cs_prelim_extra_week_final to dwhanalyst;

                    GRANT ALL ON sandbox.calendar_shift_quarter_calendar to dwhanalyst;
                    GRANT ALL ON sandbox.extra_week_parent_merchant to dwhanalyst;
                    """
sql_execute(access_script)
