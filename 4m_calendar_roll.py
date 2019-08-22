import pandas as pd
import datetime
import psycopg2

pd.set_option('expand_frame_repr', False)

#today = datetime.datetime.strptime('2017-10-20',"%Y-%m-%d")
# set prelim end date (last_data) to last wednesday. Period will "roll" on Fridays
today = datetime.date.today()
today = datetime.datetime(today.year, today.month, today.day)
if today.weekday() == 2:
	offset = 7
elif today.weekday() == 3:
	offset = 8
else:
	 offset = (today.weekday()-9)%7
last_data = today - datetime.timedelta(offset)

# pull time_period_full from vault schema. now in fake sandbox table
# con = psycopg2.connect(dbname = 'earnest4m', host = 'earnest-cluster-ssd-3m-geography.ca1y9bisuetf.us-east-1.redshift.amazonaws.com', port = '5439', user = 'dwhadmin', password = 'luckYghost96')
# cur = con.cursor()
# cur.execute("SELECT parent_merchant, label, begin_date, end_date, series, days, report_start_date, report_end_date, report_days FROM sandbox.parent_merchant_time_period_full order by parent_merchant, report_end_date")
# col_names = []
# for elt in cur.description:
# 	col_names.append(elt[0])
# time_period = pd.DataFrame(cur.fetchall(), columns = col_names)
# cur.close()
time_period= pd.read_csv("/Users/hzheng/Documents/card-configs/general/schedule_time_period.csv", sep = '|')

# adjust date datatype to datetime
time_period['begin_date'] = pd.to_datetime(time_period['begin_date'])
time_period['end_date'] = pd.to_datetime(time_period['end_date'])
time_period['report_start_date'] = pd.to_datetime(time_period['report_start_date'])
time_period['report_end_date'] = pd.to_datetime(time_period['report_end_date'])

# time_period_new will be the df for completed parent_merchant_time_period_direct
time_period_new = pd.DataFrame({'parent_merchant':[],'label':[],'begin_date':[],'end_date':[],'days':[],'series':[],'report_start_date':[],'report_end_date':[],'report_days':[]})
tp_group = time_period.groupby('parent_merchant')
# loop through calendars, check for full periods and add to time_period_new. If last data date falls within period, create prelim.
for name, group in tp_group:
    for index, row in group.iterrows():
    	if last_data > row['begin_date'] and last_data >= row['end_date']:
            time_period_new = time_period_new.append(row)
    	elif last_data >= row['begin_date'] and last_data < row['end_date']:
            label = 'Prelim' + row['label']
            series = 'Prelim' + row['series']
            year = int(row['label'][-4:])
            quarter = int(row['label'][:1])
            begin_date = row['begin_date']
            report_days = row['report_days']
            report_end_date = row['report_end_date']
            report_start_date = row['report_start_date']
            end_date = last_data
            days = (end_date - begin_date).days + 1
            prelim_new = pd.DataFrame({'parent_merchant':[name],'label':[label],'begin_date':[begin_date],'end_date':[end_date],'days':[days],'series':[series],'report_start_date':[report_start_date],'report_end_date':[report_end_date],'report_days':[report_days]})
            time_period_new = time_period_new.append(prelim_new, ignore_index = True)
            # Previous quarter prelim
            quarter_prev = str(quarter)+'Q'+str(year-1)
            prelim_prev_temp = time_period_new.loc[(time_period_new['label'] == quarter_prev) & (time_period_new['parent_merchant'] == name)]
            label_prev = 'Prelim' + prelim_prev_temp['label'].iloc[0]
            series_prev = 'Prelim' + prelim_prev_temp['series'].iloc[0]
            beginDate_prev = prelim_prev_temp['begin_date'].iloc[0]
            reportDays_prev = prelim_prev_temp['report_days'].iloc[0]
            reportStartDate_prev = prelim_prev_temp['report_start_date'].iloc[0]
            reportDays_prev = prelim_prev_temp['report_days'].iloc[0]
            reportEndDate_prev = prelim_prev_temp['report_end_date'].iloc[0]
            endDate_prev = beginDate_prev + datetime.timedelta(days - 1)
            prelim_old = pd.DataFrame({'parent_merchant':[name],'label':[label_prev],'begin_date':[beginDate_prev],'end_date':[endDate_prev],'days':[days],'series':[series_prev],'report_start_date':[reportStartDate_prev],'report_end_date':[reportEndDate_prev],'report_days':[reportDays_prev]})
            time_period_new = time_period_new.append(prelim_old, ignore_index = True)

    # edge case 1: last_date (prelim end date) does not fall within any periods, creating 0 prelims, then find next closest period and create prelim
    if len(time_period_new[(time_period_new['label'].str.contains('Prelim')==True) & (time_period_new['parent_merchant']==name)]) == 0:
        prelim_new_temp = group[(group['begin_date']-last_data).dt.days > 0][:1]
        label_new = 'Prelim' + prelim_new_temp['label'].iloc[0]
        series_new = 'Prelim' + prelim_new_temp['series'].iloc[0]
        year = int(prelim_new_temp['label'].iloc[0][-4:])
        quarter = int(prelim_new_temp['label'].iloc[0][:1])
        beginDate_new = prelim_new_temp['begin_date'].iloc[0]
        reportDays_new = prelim_new_temp['report_days'].iloc[0]
        reportStartDate_new = prelim_new_temp['report_start_date'].iloc[0]
        reportDays_new = prelim_new_temp['report_days'].iloc[0]
        reportEndDate_new = prelim_new_temp['report_end_date'].iloc[0]
        endDate_new = beginDate_new + datetime.timedelta(1)
        days = 1
        prelim_new = pd.DataFrame({'parent_merchant':[name],'label':[label_new],'begin_date':[beginDate_new],'end_date':[endDate_new],'days':[days],'series':[series_new],'report_start_date':[reportStartDate_new],'report_end_date':[reportEndDate_new],'report_days':[reportDays_new]})
        time_period_new = time_period_new.append(prelim_new, ignore_index = True)
        # previous quarter prelim
        quarter_prev = str(quarter)+'Q'+str(year-1)
        prelim_prev_temp = time_period_new.loc[(time_period_new['label'] == quarter_prev) & (time_period_new['parent_merchant']==name)]
        label_prev = 'Prelim' + prelim_prev_temp['label'].iloc[0]
        series_prev = 'Prelim' + prelim_prev_temp['series'].iloc[0]
        beginDate_prev = prelim_prev_temp['begin_date'].iloc[0]
        reportDays_prev = prelim_prev_temp['report_days'].iloc[0]
        reportStartDate_prev = prelim_prev_temp['report_start_date'].iloc[0]
        reportDays_prev = prelim_prev_temp['report_days'].iloc[0]
        reportEndDate_prev = prelim_prev_temp['report_end_date'].iloc[0]
        endDate_prev = beginDate_prev + datetime.timedelta(1)
        days = 1
        prelim_old = pd.DataFrame({'parent_merchant':[name],'label':[label_prev],'begin_date':[beginDate_prev],'end_date':[endDate_prev],'days':[days],'series':[series_prev],'report_start_date':[reportStartDate_prev],'report_end_date':[reportEndDate_prev],'report_days':[reportDays_prev]})
        time_period_new = time_period_new.append(prelim_old, ignore_index = True)
    #edge case 2: last date falls within two different periods, creating 4 prelims. Need to remove the earlier of the two pairs
    elif len(time_period_new[(time_period_new['label'].str.contains('Prelim')==True) & (time_period_new['parent_merchant']==name)])==4:
        time_period_new = time_period_new.drop(time_period_new[(time_period_new['label'].str.contains('Prelim')==True) & (time_period_new['parent_merchant']==name)].sort_values('report_end_date').iloc[[-1,1]].index)

# reorder columns
time_period_new[['days','report_days']] = time_period_new[['days','report_days']].astype(int)
time_period_new = time_period_new.sort_values(by=['parent_merchant','report_end_date','end_date','label'], ascending =[True,True,True,False])
time_period_new = time_period_new[['parent_merchant','label','begin_date', 'end_date','series', 'days','report_start_date','report_end_date','report_days']]

# for now, writing to csv. In practice, would write to vault.parent_merchant_time_period_direct
time_period_new.to_csv('/Users/hzheng/Documents/card-configs/hzheng/parent_merchant_time_period_{}.csv'.format(today.strftime("%Y%m%d")), index=False, sep = '|', date_format='%-m/%-d/%Y')