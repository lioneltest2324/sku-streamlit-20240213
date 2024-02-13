import streamlit as st
import pandas as pd
from universal_component_for_campaign import load_and_process_data,process_usfeed_and_hmfeed_sku_on_ads_data,process_hk_cost_and_value_on_ads_data,\
    process_old_new_sku_2022_and_2023_on_ads_data,merged_spu_to_sku_on_ads_data,merged_imagelink_to_sku_on_ads_data,create_date_filtered_df,\
    output_groupby_df,out_date_range_data,add_groupby_sum_columns_to_list_df,create_dynamic_column_setting,add_custom_proportion_to_df,add_custom_proportion_to_df_x100,\
    create_sensor_gmv_filter_input,create_bulk_sku_input,create_sensor_campaign_filter_input_df,condition_evaluate,merged_saleprice_to_sku_on_ads_data,\
    create_compare_summary_df,format_first_two_rows,format_comparison,colorize_comparison
st.set_page_config(layout="wide")
# ---------------------------------------------------------------------基础数据处理区开始---------------------------------------------------------------------------------------------------
sensor_url = 'https://docs.google.com/spreadsheets/d/1X0YPC6iAZn1Lu4szX67fi5h4B8HiVbfA-i68EyzpOq0/edit#gid=0'
ads_url = 'https://docs.google.com/spreadsheets/d/13G1sZWVLKa_kpScqGVmNp-5abCTkxmAFW0dxW29DMUY/edit#gid=0'
spu_index_url = "https://docs.google.com/spreadsheets/d/1bQTrtNC-o9etJ3xUwMeyD8m383xRRq9U7a3Y-gxjP-U/edit#gid=455883801"

ads_daily_df = load_and_process_data(ads_url,0)
sensor_daily = load_and_process_data(sensor_url,0)
spu_index = load_and_process_data(spu_index_url,455883801)
old_new = load_and_process_data(spu_index_url,666585210)

ads_daily_df= process_usfeed_and_hmfeed_sku_on_ads_data(ads_daily_df,'MC ID',569301767,9174985,'SKU')
ads_daily_df= process_hk_cost_and_value_on_ads_data(ads_daily_df,'Currency','cost','ads value','HKD')
ads_daily_df = process_old_new_sku_2022_and_2023_on_ads_data(ads_daily_df,'customlabel1')
ads_daily_df['SKU'] = ads_daily_df['SKU'].str.strip().str.replace('\n', '').replace('\t', '').str.upper()
ads_daily_df = merged_spu_to_sku_on_ads_data(ads_daily_df,spu_index,'SKU', 'SPU')
old_new  = old_new.rename(columns={'SKU ID':'SKU'})
sensor_daily  = sensor_daily.rename(columns={'行为时间':'Date'})
ads_daily_df = merged_imagelink_to_sku_on_ads_data(ads_daily_df,old_new,'SKU', 'imagelink')
ads_daily_df = merged_saleprice_to_sku_on_ads_data(ads_daily_df,old_new,'SKU', 'Sale Price')
sensor_daily = merged_spu_to_sku_on_ads_data(sensor_daily,spu_index,'SKU', 'SPU')
# 批量GMV筛选框
and_tags,or_tags,exclude_tags = create_sensor_gmv_filter_input('GMV条件输入(广告系列筛选)')

with st.sidebar:
    # 日期选择框
    selected_range = out_date_range_data(ads_daily_df,'Date',"自选日期范围")
    compare_selected_range = out_date_range_data(ads_daily_df,'Date',"对比数据日期范围")
    # 批量SKU输入框
    sku_tags = create_bulk_sku_input('sku_text', 'sku_saved_text', "批量输入SKU(一个SKU一行)")

# 选择日期范围内的数据
sensor_daily['Date'] = pd.to_datetime(sensor_daily['Date'])
ads_daily_df['Date'] = pd.to_datetime(ads_daily_df['Date'])
# 处理普通选择日期范围内的数据
seonsor_daily_filtered_date_range_df = create_date_filtered_df(sensor_daily,'Date',selected_range)
ads_daily_filtered_date_range_df = create_date_filtered_df(ads_daily_df,'Date',selected_range)
ads_daily_filtered_date_range_df = output_groupby_df(ads_daily_filtered_date_range_df,
['SKU', 'SPU', 'Date', 'Product Type 1', 'Product Type 2', 'Product Type 3','old_or_new', 'imagelink','Sale Price'],
['impression', 'cost', 'click', 'conversions', 'ads value'], 'sum').reset_index()
ads_daily_filtered_date_range_df['日期范围'] = selected_range[0].strftime('%Y-%m-%d')+"至"+selected_range[1].strftime('%Y-%m-%d')
seonsor_daily_filtered_date_range_df['日期范围'] = selected_range[0].strftime('%Y-%m-%d')+"至"+selected_range[1].strftime('%Y-%m-%d')
seonsor_daily_filtered_date_range_df['Date'] = seonsor_daily_filtered_date_range_df['Date'].dt.strftime('%Y-%m-%d')
ads_daily_filtered_date_range_df['Date'] = ads_daily_filtered_date_range_df['Date'].dt.strftime('%Y-%m-%d')
# ---------------------------------------------------------------------对比时间数据处理开始---------------------------------------------------------------------------------------------------
compare_seonsor_daily_filtered_date_range_df = create_date_filtered_df(sensor_daily,'Date',compare_selected_range)
compare_ads_daily_filtered_date_range_df = create_date_filtered_df(ads_daily_df,'Date',compare_selected_range)
compare_ads_daily_filtered_date_range_df = output_groupby_df(compare_ads_daily_filtered_date_range_df,
['SKU', 'SPU', 'Date', 'Product Type 1', 'Product Type 2', 'Product Type 3','old_or_new', 'imagelink','Sale Price'],
['impression', 'cost', 'click', 'conversions', 'ads value'], 'sum').reset_index()
compare_ads_daily_filtered_date_range_df['日期范围'] = compare_selected_range[0].strftime('%Y-%m-%d')+"至"+compare_selected_range[1].strftime('%Y-%m-%d')
compare_seonsor_daily_filtered_date_range_df['日期范围'] = compare_selected_range[0].strftime('%Y-%m-%d')+"至"+compare_selected_range[1].strftime('%Y-%m-%d')
compare_seonsor_daily_filtered_date_range_df['Date'] = compare_seonsor_daily_filtered_date_range_df['Date'].dt.strftime('%Y-%m-%d')
compare_ads_daily_filtered_date_range_df['Date'] = compare_ads_daily_filtered_date_range_df['Date'].dt.strftime('%Y-%m-%d')
# ---------------------------------------------------------------------对比时间数据处理结束---------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------基础数据处理区结束---------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------条件判断区开始---------------------------------------------------------------------------------------------------

def compare_summary_df_output(summary_df,compare_summary_df,select_column):
    summary_df = output_groupby_df(summary_df,['日期范围'],
    ['impression', 'cost', 'click', 'conversions', 'ads value', 'GMV', 'UV', 'AddtoCart','saleuser', 'sale'], 'sum').reset_index()
    summary_df = add_custom_proportion_to_df(summary_df, 'GMV', 'cost', '神策ROI')
    summary_df = add_custom_proportion_to_df(summary_df, 'ads value', 'cost', 'ads ROI')
    summary_df = add_custom_proportion_to_df(summary_df, 'cost', 'conversions', 'ads CPA')
    summary_df = add_custom_proportion_to_df(summary_df, 'cost', 'click', 'CPC')
    summary_df = add_custom_proportion_to_df(summary_df, 'click', 'impression', 'CTR')
    summary_df = add_custom_proportion_to_df(summary_df, 'sale', 'UV', '神策转化率')
    summary_df = add_custom_proportion_to_df(summary_df, 'AddtoCart', 'UV', '神策加购率')
    summary_df = add_custom_proportion_to_df(summary_df, 'GMV', 'saleuser', '客单价')
    compare_summary_df = output_groupby_df(compare_summary_df,['日期范围'],
    ['impression', 'cost', 'click', 'conversions', 'ads value', 'GMV', 'UV', 'AddtoCart','saleuser', 'sale'], 'sum').reset_index()
    compare_summary_df = add_custom_proportion_to_df(compare_summary_df, 'GMV', 'cost', '神策ROI')
    compare_summary_df = add_custom_proportion_to_df(compare_summary_df, 'ads value', 'cost', 'ads ROI')
    compare_summary_df = add_custom_proportion_to_df(compare_summary_df, 'cost', 'conversions', 'ads CPA')
    compare_summary_df = add_custom_proportion_to_df(compare_summary_df, 'cost', 'click', 'CPC')
    compare_summary_df = add_custom_proportion_to_df(compare_summary_df, 'click', 'impression', 'CTR')
    compare_summary_df = add_custom_proportion_to_df(compare_summary_df, 'sale', 'UV', '神策转化率')
    compare_summary_df = add_custom_proportion_to_df(compare_summary_df, 'AddtoCart', 'UV', '神策加购率')
    compare_summary_df = add_custom_proportion_to_df(compare_summary_df, 'GMV', 'saleuser', '客单价')
    combined_df = create_compare_summary_df(summary_df,compare_summary_df,select_column)
    formatted_df = combined_df.head(2).copy()
    formatted_df[['sale', 'saleuser', 'UV', 'AddtoCart', 'impression','click']] = formatted_df[['sale', 'saleuser', 'UV', 'AddtoCart', 'impression','click']].astype(int)
    for column in formatted_df.head():
        format_string = '{:.2f}' if column in ['cost', 'GMV', 'ads value', 'CPC', 'conversions','ads ROI','神策ROI','ads CPA','客单价'] else '{}'
        format_string = '{:.2%}' if column in ['CTR', '神策转化率', '神策加购率'] else format_string
        formatted_df[column] = formatted_df[column].apply(format_first_two_rows, args=(format_string,))
    compare_data_df = combined_df.iloc[2:3].copy()
    compare_data_df[compare_data_df.columns[1:]] = compare_data_df[compare_data_df.columns[1:]].apply(pd.to_numeric,errors='coerce')
    combined_df.update(formatted_df)
    combined_df.update(compare_data_df)
    summary_options = st.multiselect(
        '选择汇总数据维度',
        combined_df.columns,
        ['日期范围', 'cost', 'click', 'GMV', 'ads value', 'CPC', 'conversions']
    )
    combined_df = combined_df[summary_options]
    combined_df = combined_df.apply(format_comparison, axis=1)
    combined_df = combined_df.style.apply(colorize_comparison, axis=1)
    st.subheader('对比数据')
    st.dataframe(combined_df,
                 width=1600, height=200)

def summary_df_output(summary_df):
    summary_df = output_groupby_df(summary_df,['SKU', 'SPU', 'Product Type 1', 'Product Type 2', 'Product Type 3', 'old_or_new','imagelink', 'Sale Price'],
    ['impression', 'cost', 'click', 'conversions', 'ads value', 'GMV', 'UV', 'AddtoCart','saleuser', 'sale'], 'sum').reset_index()
    summary_df = add_custom_proportion_to_df(summary_df, 'GMV', 'cost', '神策ROI')
    summary_df = add_custom_proportion_to_df(summary_df, 'ads value', 'cost', 'ads ROI')
    summary_df = add_custom_proportion_to_df(summary_df, 'cost', 'conversions', 'ads CPA')
    summary_df = add_custom_proportion_to_df(summary_df, 'cost', 'click', 'CPC')
    summary_df = add_custom_proportion_to_df_x100(summary_df, 'click', 'impression', 'CTR')
    summary_df = add_custom_proportion_to_df_x100(summary_df, 'sale', 'UV', '神策转化率')
    summary_df = add_custom_proportion_to_df_x100(summary_df, 'AddtoCart', 'UV', '神策加购率')
    summary_df = add_custom_proportion_to_df_x100(summary_df, 'GMV', 'saleuser', '客单价')
    summary_df_column_config = create_dynamic_column_setting(summary_df,['SKU', 'SPU', 'Date', 'Product Type 1', 'Product Type 2','Product Type 3', 'old_or_new', 'Sale Price'],
    ['imagelink'],['conversions', 'ads value', 'GMV', 'AddtoCart','saleuser', '神策ROI', 'ads ROI', 'CPC', 'ads CPA', '客单价', 'cost'],['CTR','神策转化率', '神策加购率'], ['impression', 'click', 'sale', 'UV'], None, None)
    summary_df_options = st.multiselect(
        '选择三级类目占比数据维度',
        summary_df.columns,
        ['SKU', 'Product Type 3', 'Sale Price', 'old_or_new', 'imagelink', 'impression', 'CPC', 'cost', 'ads CPA',
         'ads ROI',
         'conversions', 'GMV', 'sale']
    )
    # 三级类目筛选框
    unique_category_3 = ads_daily_df['Product Type 3'].unique()
    category_3_options = st.multiselect(
        '选择三级类目',
        unique_category_3
    )
    if category_3_options:
        st.dataframe(summary_df[summary_df_options][summary_df['Product Type 3'].isin(category_3_options)].set_index(
            ['SKU', 'Product Type 3']),
            column_config=summary_df_column_config
            , width=1600, height=400)
    else:
        st.dataframe(summary_df[summary_df_options].set_index(['SKU', 'Product Type 3']),
                     column_config=summary_df_column_config
                     , width=1600, height=400)

def create_true_gmv_false_sku_df():
   seonsor_daily_filter_input_df = create_sensor_campaign_filter_input_df(seonsor_daily_filtered_date_range_df,and_tags,or_tags,exclude_tags,'Campaign')
   seonsor_remove_capaign_sum_df = output_groupby_df(seonsor_daily_filter_input_df, ['SKU', 'SPU', 'Date'],['UV', 'AddtoCart', 'saleuser', 'sale', 'GMV'],'sum').reset_index()
   summary_df = pd.merge(ads_daily_filtered_date_range_df,seonsor_remove_capaign_sum_df[['Date','SKU','GMV','UV','AddtoCart','saleuser','sale']],on=['Date','SKU'],how='left')
   summary_df_output(summary_df)
   compare_seonsor_daily_filter_input_df = create_sensor_campaign_filter_input_df(compare_seonsor_daily_filtered_date_range_df,and_tags,or_tags,exclude_tags,'Campaign')
   compare_seonsor_remove_capaign_sum_df = output_groupby_df(compare_seonsor_daily_filter_input_df, ['SKU', 'SPU', 'Date'],['UV', 'AddtoCart', 'saleuser', 'sale', 'GMV'],'sum').reset_index()
   compare_summary_df = pd.merge(compare_ads_daily_filtered_date_range_df,compare_seonsor_remove_capaign_sum_df[['Date','SKU','GMV','UV','AddtoCart','saleuser','sale']],on=['Date','SKU'],how='left')
   compare_summary_df_output(summary_df, compare_summary_df,
    ['日期范围','impression', 'cost', 'click', 'conversions', 'ads value', 'GMV','UV','AddtoCart','saleuser','sale','神策ROI','ads ROI','ads CPA','CPC','CTR','神策转化率','神策加购率'])

def create_false_gmv_false_sku_df():
   seonsor_remove_capaign_sum_df = output_groupby_df(seonsor_daily_filtered_date_range_df,['SKU', 'SPU', 'Date'],['UV','AddtoCart','saleuser','sale','GMV'],'sum').reset_index()
   summary_df = pd.merge(ads_daily_filtered_date_range_df,seonsor_remove_capaign_sum_df[['Date', 'SKU', 'GMV', 'UV', 'AddtoCart', 'saleuser', 'sale']],on=['Date', 'SKU'], how='left')
   summary_df_output(summary_df)
   compare_seonsor_remove_capaign_sum_df = output_groupby_df(compare_seonsor_daily_filtered_date_range_df,['SKU', 'SPU', 'Date'],['UV','AddtoCart','saleuser','sale','GMV'],'sum').reset_index()
   compare_summary_df = pd.merge(compare_ads_daily_filtered_date_range_df,compare_seonsor_remove_capaign_sum_df[['Date', 'SKU', 'GMV', 'UV', 'AddtoCart', 'saleuser', 'sale']],on=['Date', 'SKU'], how='left')
   compare_summary_df_output(summary_df, compare_summary_df,
    ['日期范围','impression', 'cost', 'click', 'conversions', 'ads value', 'GMV','UV','AddtoCart','saleuser','sale','神策ROI','ads ROI','ads CPA','CPC','CTR','神策转化率','神策加购率'])

def create_true_gmv_true_sku_df():
    seonsor_daily_filter_input_df = create_sensor_campaign_filter_input_df(seonsor_daily_filtered_date_range_df,and_tags, or_tags, exclude_tags, 'Campaign')
    seonsor_daily_filter_input_sku_tags_input_df = seonsor_daily_filter_input_df[seonsor_daily_filter_input_df['SKU'].isin(sku_tags)]
    seonsor_remove_capaign_sum_df = output_groupby_df(seonsor_daily_filter_input_sku_tags_input_df, ['SKU', 'SPU', 'Date'],
    ['UV', 'AddtoCart', 'saleuser', 'sale', 'GMV'],'sum').reset_index()
    ads_daily_filter_input_df = ads_daily_filtered_date_range_df[ads_daily_filtered_date_range_df['SKU'].isin(sku_tags)]
    summary_df = pd.merge(ads_daily_filter_input_df,seonsor_remove_capaign_sum_df[['Date', 'SKU', 'GMV', 'UV', 'AddtoCart', 'saleuser', 'sale']],on=['Date', 'SKU'], how='left')
    summary_df_output(summary_df)

    summary_df = output_groupby_df(summary_df, ['Date','日期范围'],['impression', 'cost', 'click', 'conversions', 'ads value', 'GMV', 'UV', 'AddtoCart','saleuser', 'sale'], 'sum').reset_index()
    summary_df = add_custom_proportion_to_df(summary_df, 'GMV', 'cost', '神策ROI')
    summary_df = add_custom_proportion_to_df(summary_df, 'ads value', 'cost', 'ads ROI')
    summary_df = add_custom_proportion_to_df(summary_df, 'cost', 'conversions', 'ads CPA')
    summary_df = add_custom_proportion_to_df(summary_df, 'cost', 'click', 'CPC')
    summary_df = add_custom_proportion_to_df_x100(summary_df, 'click', 'impression', 'CTR')
    summary_df = add_custom_proportion_to_df_x100(summary_df, 'sale', 'UV', '神策转化率')
    summary_df = add_custom_proportion_to_df_x100(summary_df, 'AddtoCart', 'UV', '神策加购率')
    summary_df = add_custom_proportion_to_df_x100(summary_df, 'GMV', 'saleuser', '客单价')
    summary_df_column_config = create_dynamic_column_setting(summary_df,['SKU', 'SPU', 'Date','日期范围','Product Type 1', 'Product Type 2','Product Type 3', 'old_or_new', 'Sale Price','日期范围'],
    ['imagelink'],['conversions', 'ads value', 'GMV', 'AddtoCart','saleuser', '神策ROI', 'ads ROI', 'CPC', 'ads CPA', '客单价', 'cost'],['CTR','神策转化率', '神策加购率'], ['impression', 'click', 'sale', 'UV'], None, None)
    st.subheader('日趋势')
    st.dataframe(summary_df.drop(columns=['日期范围'])
    ,column_config=summary_df_column_config)

    compare_seonsor_daily_filter_input_df = create_sensor_campaign_filter_input_df(compare_seonsor_daily_filtered_date_range_df,and_tags, or_tags, exclude_tags, 'Campaign')
    compare_seonsor_daily_filter_input_sku_tags_input_df = compare_seonsor_daily_filter_input_df[compare_seonsor_daily_filter_input_df['SKU'].isin(sku_tags)]
    compare_seonsor_remove_capaign_sum_df = output_groupby_df(compare_seonsor_daily_filter_input_sku_tags_input_df, ['SKU', 'SPU', 'Date'],
    ['UV', 'AddtoCart', 'saleuser', 'sale', 'GMV'],'sum').reset_index()
    compare_ads_daily_filter_input_df = compare_ads_daily_filtered_date_range_df[compare_ads_daily_filtered_date_range_df['SKU'].isin(sku_tags)]
    compare_summary_df = pd.merge(compare_ads_daily_filter_input_df,compare_seonsor_remove_capaign_sum_df[['Date', 'SKU', 'GMV', 'UV', 'AddtoCart', 'saleuser', 'sale']],on=['Date', 'SKU'], how='left')
    compare_summary_df_output(summary_df, compare_summary_df,['日期范围', 'impression', 'cost', 'click', 'conversions', 'ads value', 'GMV', 'UV',
    'AddtoCart', 'saleuser', 'sale', '神策ROI', 'ads ROI', 'ads CPA', 'CPC', 'CTR','神策转化率', '神策加购率'])

def create_false_gmv_true_sku_df():
    seonsor_daily_filter_input_sku_tags_input_df = seonsor_daily_filtered_date_range_df[seonsor_daily_filtered_date_range_df['SKU'].isin(sku_tags)]
    seonsor_remove_capaign_sum_df = output_groupby_df(seonsor_daily_filter_input_sku_tags_input_df,['SKU', 'SPU', 'Date'],
    ['UV', 'AddtoCart', 'saleuser', 'sale', 'GMV'],'sum').reset_index()
    ads_daily_filter_input_df = ads_daily_filtered_date_range_df[ads_daily_filtered_date_range_df['SKU'].isin(sku_tags)]
    summary_df = pd.merge(ads_daily_filter_input_df,seonsor_remove_capaign_sum_df[['Date', 'SKU', 'GMV', 'UV', 'AddtoCart', 'saleuser', 'sale']],on=['Date', 'SKU'], how='left')
    summary_df_output(summary_df)
    summary_df = output_groupby_df(summary_df,['Date','日期范围'],['impression', 'cost', 'click', 'conversions', 'ads value', 'GMV', 'UV', 'AddtoCart','saleuser', 'sale'], 'sum').reset_index()
    summary_df = add_custom_proportion_to_df(summary_df, 'GMV', 'cost', '神策ROI')
    summary_df = add_custom_proportion_to_df(summary_df, 'ads value', 'cost', 'ads ROI')
    summary_df = add_custom_proportion_to_df(summary_df, 'cost', 'conversions', 'ads CPA')
    summary_df = add_custom_proportion_to_df(summary_df, 'cost', 'click', 'CPC')
    summary_df = add_custom_proportion_to_df_x100(summary_df, 'click', 'impression', 'CTR')
    summary_df = add_custom_proportion_to_df_x100(summary_df, 'sale', 'UV', '神策转化率')
    summary_df = add_custom_proportion_to_df_x100(summary_df, 'AddtoCart', 'UV', '神策加购率')
    summary_df = add_custom_proportion_to_df_x100(summary_df, 'GMV', 'saleuser', '客单价')
    summary_df_column_config = create_dynamic_column_setting(summary_df,['SKU', 'SPU', 'Date','日期范围', 'Product Type 1', 'Product Type 2','Product Type 3', 'old_or_new', 'Sale Price'],
    ['imagelink'],['conversions', 'ads value', 'GMV', 'AddtoCart','saleuser', '神策ROI', 'ads ROI', 'CPC', 'ads CPA', '客单价', 'cost'],['CTR','神策转化率', '神策加购率'], ['impression', 'click', 'sale', 'UV'], None, None)
    st.subheader('日趋势')
    st.dataframe(summary_df.drop(columns=['日期范围'])
    ,column_config=summary_df_column_config)

    compare_seonsor_daily_filter_input_sku_tags_input_df = compare_seonsor_daily_filtered_date_range_df[compare_seonsor_daily_filtered_date_range_df['SKU'].isin(sku_tags)]
    compare_seonsor_remove_capaign_sum_df = output_groupby_df(compare_seonsor_daily_filter_input_sku_tags_input_df,['SKU', 'SPU', 'Date'],
    ['UV', 'AddtoCart', 'saleuser', 'sale', 'GMV'],'sum').reset_index()
    compare_ads_daily_filter_input_df = compare_ads_daily_filtered_date_range_df[compare_ads_daily_filtered_date_range_df['SKU'].isin(sku_tags)]
    compare_summary_df = pd.merge(compare_ads_daily_filter_input_df,compare_seonsor_remove_capaign_sum_df[['Date', 'SKU', 'GMV', 'UV', 'AddtoCart', 'saleuser', 'sale']],on=['Date', 'SKU'], how='left')
    compare_summary_df_output(summary_df, compare_summary_df,['日期范围', 'impression', 'cost', 'click', 'conversions', 'ads value', 'GMV', 'UV',
    'AddtoCart', 'saleuser', 'sale', '神策ROI', 'ads ROI', 'ads CPA', 'CPC', 'CTR','神策转化率', '神策加购率'])

# ----------------------------------------------------------------------条件判断区结束---------------------------------------------------------------------------------------------------

# 调用判断
condition_evaluate(
# sku_tags开始判断
    sku_tags,
# sku_tags为true
    lambda: condition_evaluate(
# and_tags or or_tags or exclude_tags开始判断
    and_tags or or_tags or exclude_tags,
# and_tags or or_tags or exclude_tags存在任何一个
    lambda: create_true_gmv_true_sku_df(),
# and_tags or or_tags or exclude_tags都不存在
    lambda: create_false_gmv_true_sku_df(),
    ),
# sku_tags为false
    lambda: condition_evaluate(
# and_tags or or_tags or exclude_tags开始判断
    and_tags or or_tags or exclude_tags,
# and_tags or or_tags or exclude_tags存在任何一个
    lambda: create_true_gmv_false_sku_df(),
# and_tags or or_tags or exclude_tags都不存在
    lambda: create_false_gmv_false_sku_df())
)
