# Databricks notebook source
startDate = dbutils.widgets.get('startDate')
endDate = dbutils.widgets.get('endDate')
import datetime
begin_date=datetime.datetime.strptime(startDate, '%Y-%m-%d')
end_date = datetime.datetime.strptime(endDate,'%Y-%m-%d')
# end_date = datetime.datetime.strptime('20180201','%Y%m%d')
while begin_date < end_date:
    dateStr = datetime.datetime.strftime(begin_date, '%Y%m%d')
    try:
        filels = dbutils.fs.ls("/mnt/stcdpstgn3/csv/tradeSyn/"+dateStr)
    except Exception as err:
        begin_date += datetime.timedelta(days=1)
        continue
    for file in filels:
        print(file.path[5:])
        df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("sep", "|").load(file.path[5:])
        df.createOrReplaceTempView("tmp_tradeSyn")
        spark.sql("select count(*) from tmp_tradeSyn").show()
        spark.sql(""" insert into ods_tradesyn (primary_id, sg_exclusive_shop_id, sg_finish_shop_id, discount_fee, pay_time, sys_trade_id, buyer_rate, num, ext_json, receiver_province, receiver_city, consign_time, modify_time, pay_type, receiver_phone, group_end_time, sg_share_shop_id, payment, id, shop_id, adjust_fee, receiver_zip, sg_finish_guide_id, created, activity_no, customer_name, out_sid, receiver_address, out_trade_id, out_alias, trade_status, remark_sign, sg_share_guide_id, available_confirm_fee, seller_memo, customer_mobile, buyer_memo, real_point_fee, sg_handle_shop_id, shipping_type, shop_name, lading_code, platform, sg_exclusive_guide_id, out_nick, is_all_refunding, receiver_district, step_paid_fee, out_company_name, trade_type, sg_handle_guide_id, trade_memo, sys_customer_id, receiver_name, post_fee, receiver_mobile, timeout_action_time, update_time, group_status, buyer_message, trade_from, total_fee, out_state, end_time, sv_payment, syn_date) select primaryId, sgExclusiveShopId, sgFinishShopId, discountFee, payTime, sysTradeId, buyerRate, num, extJson, receiverProvince, receiverCity, consignTime, modifyTime, payType, receiverPhone, groupEndTime, sgShareShopId, payment, id, shopId, adjustFee, receiverZip, sgFinishGuideId, created, activityNo, customerName, outSid, receiverAddress, outTradeId, outAlias, tradeStatus, remarkSign, sgShareGuideId, availableConfirmFee, sellerMemo, customerMobile, buyerMemo, realPointFee, sgHandleShopId, shippingType, shopName, ladingCode, platform, sgExclusiveGuideId, outNick, isAllRefunding, receiverDistrict, stepPaidFee, outCompanyName, tradeType, sgHandleGuideId, tradeMemo, sysCustomerId, receiverName, postFee, receiverMobile, timeoutActionTime, updateTime, groupStatus, buyerMessage, tradeFrom, totalFee, outState, endTime, svPayment, now() from tmp_tradeSyn""")
    begin_date += datetime.timedelta(days=1)
    

# COMMAND ----------

# MAGIC %fs ls /mnt

# COMMAND ----------

import logging
filename ="/Users/zhihuiwang/Downloads/log/qt.log"
logger = logging.getLogger(filename)
