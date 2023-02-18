# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "056f610a-be7a-4193-956e-09a379667a40",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="dbwscope",key="nasclientsecret"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.chinacloudapi.cn/6a596574-1518-4214-840e-216bb42592e7/oauth2/token"}
# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
    source = "abfss://csv@stcdpstgntt.dfs.core.chinacloudapi.cn/",
    mount_point = "/mnt/stcdpstgntt/csv",
    extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/stcdpstgn3

# COMMAND ----------

 ping stcdpstgn3.dfs.core.chinacloudapi.cn

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.unmount("/mnt/stcdpstgn3/script/", true)

# COMMAND ----------

# MAGIC %md
# MAGIC 下面是ods_activateCustomerListSync的数据导入

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ods_activateCustomerListSync (
# MAGIC activate_time	string,
# MAGIC address	string,
# MAGIC asset_arr	string,
# MAGIC asset_json	string,
# MAGIC baby_birthday	string,
# MAGIC baby_name	string,
# MAGIC baby_sex	integer,
# MAGIC bind_time	string,
# MAGIC birthday	string,
# MAGIC buyer_alipay_no	string,
# MAGIC card_receive_time	string,
# MAGIC city	string,
# MAGIC country	string,
# MAGIC customer_from	integer,
# MAGIC customer_head_image	string
# MAGIC ) location "/mnt/stcdpstgn3/table//ods_activateCustomerListSync"

# COMMAND ----------

import datetime
begin_date=datetime.datetime.strptime('20160801', '%Y%m%d')
end_date = datetime.datetime.strptime('20230116','%Y%m%d')
# end_date = datetime.datetime.strptime('20160901','%Y%m%d')
while begin_date <= end_date:
    dateStr = datetime.datetime.strftime(begin_date, '%Y%m%d')
    try:
        filels = dbutils.fs.ls("/mnt/stcdpstgn3/csv/activateCustomerListSync/"+dateStr)
    except Exception as err:
        begin_date += datetime.timedelta(days=1)
        continue
    for file in filels:
        print(file.path[5:])
        df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("sep", "|").load(file.path[5:])
        df.createOrReplaceTempView("tmp_activateCustomerListSync")
        spark.sql("select count(*) from tmp_activateCustomerListSync").show()
        spark.sql(""" insert into ods_activateCustomerListSync (primary_id, country, right_black_str, inviter_info_list, mix_mobile, discount, ext_json, card_receive_time, customer_from, source, sg_guide_id, marry_status, sv_mobile, asset_arr, province, grade_relegation_time, asset_json, id, shop_id, sg_recruit_shop_id, last_concern_time, qq, activate_time, is_member_black, customer_name, out_alias, district, grade, sg_recruit_guide_id, user_type, is_activate, birthday, fans_status, sub_platform, baby_birthday, city, buyer_alipay_no, member_card, platform, nick, tel_phone, bind_time, customer_id, is_t_b_member_ship, email, customer_head_image, grade_name, mix_mobile_list, address, customer_remark, out_shop_id, sex, mobile, update_time, view_id, baby_sex, shop_customer_vo_list, baby_name, sg_shop_id, infos, is_unsubscribe, syn_date) select primaryId, country, rightBlackStr, inviterInfoList, mixMobile, discount, extJson, cardReceiveTime, customerFrom, source, sgGuideId, marryStatus, svMobile, assetArr, province, gradeRelegationTime, assetJson, id, shopId, sgRecruitShopId, lastConcernTime, qq, activateTime, isMemberBlack, customerName, outAlias, district, grade, sgRecruitGuideId, userType, isActivate, birthday, fansStatus, subPlatform, babyBirthday, city, buyerAlipayNo, memberCard, platform, nick, telPhone, bindTime, customerId, isTBMemberShip, email, customerHeadImage, gradeName, mixMobileList, address, customerRemark, outShopId, sex, mobile, updateTime, viewId, babySex, shopCustomerVoList, babyName, sgShopId, infos, isUnsubscribe, now() from tmp_activateCustomerListSync""")
    begin_date += datetime.timedelta(days=1)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ods_activateCustomerListSync ;

# COMMAND ----------

# MAGIC %md
# MAGIC 下面是ods_tradesyn的数据导入

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ods_tradesyn (
# MAGIC primary_id string,
# MAGIC sg_exclusive_shop_id string,
# MAGIC sg_finish_shop_id string,
# MAGIC discount_fee string,
# MAGIC pay_time string,
# MAGIC sys_trade_id string,
# MAGIC buyer_rate string,
# MAGIC num string,
# MAGIC ext_json string,
# MAGIC receiver_province string,
# MAGIC receiver_city string,
# MAGIC consign_time string,
# MAGIC modify_time string,
# MAGIC pay_type string,
# MAGIC receiver_phone string,
# MAGIC group_end_time string,
# MAGIC sg_share_shop_id string,
# MAGIC payment string,
# MAGIC id string,
# MAGIC shop_id string,
# MAGIC adjust_fee string,
# MAGIC receiver_zip string,
# MAGIC sg_finish_guide_id string,
# MAGIC created string,
# MAGIC activity_no string,
# MAGIC customer_name string,
# MAGIC out_sid string,
# MAGIC receiver_address string,
# MAGIC out_trade_id string,
# MAGIC out_alias string,
# MAGIC trade_status string,
# MAGIC remark_sign string,
# MAGIC sg_share_guide_id string,
# MAGIC available_confirm_fee string,
# MAGIC seller_memo string,
# MAGIC customer_mobile string,
# MAGIC buyer_memo string,
# MAGIC real_point_fee string,
# MAGIC sg_handle_shop_id string,
# MAGIC shipping_type string,
# MAGIC shop_name string,
# MAGIC lading_code string,
# MAGIC platform string,
# MAGIC sg_exclusive_guide_id string,
# MAGIC out_nick string,
# MAGIC is_all_refunding string,
# MAGIC receiver_district string,
# MAGIC step_paid_fee string,
# MAGIC out_company_name string,
# MAGIC trade_type string,
# MAGIC sg_handle_guide_id string,
# MAGIC trade_memo string,
# MAGIC sys_customer_id string,
# MAGIC receiver_name string,
# MAGIC post_fee string,
# MAGIC receiver_mobile string,
# MAGIC timeout_action_time string,
# MAGIC update_time string,
# MAGIC group_status string,
# MAGIC buyer_message string,
# MAGIC trade_from string,
# MAGIC total_fee string,
# MAGIC out_state string,
# MAGIC end_time string,
# MAGIC sv_payment string,
# MAGIC syn_date string
# MAGIC ) location "/mnt/stcdpstgntt/table/ods_tradesyn"

# COMMAND ----------

import datetime
begin_date=datetime.datetime.strptime('20180101', '%Y%m%d')
end_date = datetime.datetime.strptime('20230116','%Y%m%d')
# end_date = datetime.datetime.strptime('20180201','%Y%m%d')
while begin_date <= end_date:
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

# MAGIC %sql
# MAGIC --delete from ods_tradesyn where syn_date > '2023-01-18';
# MAGIC select count(1) from ods_tradesyn where syn_date > '2023-01-18';

# COMMAND ----------

# MAGIC %sql
# MAGIC --delete from ods_activateCustomerListSync where syn_date > '2023-01-18';
# MAGIC select count(1) from ods_activateCustomerListSync where syn_date > '2023-01-18';

# COMMAND ----------

# MAGIC %sql
# MAGIC select round(avg(sex) over win, 1) as sexavg, min(sex) over win as minSex, max(sex) over win as maxSex from ods_activateCustomerListSync window win as (order by sex rows between 2 preceding and 2 following)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ods_activateCustomerListSync limit 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ods_activateCustomerListSync limit 100;

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.sql("select * from ods_activateCustomerListSync");
# MAGIC df.write.mode(SaveMode.Append).saveAsTable("dwd_activateCustomerListSync")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from dwd_activateCustomerListSync;

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
# MAGIC val file_location = "/mnt/stcdpstgn3/activateCustomerListSync/20230105/activateCustomerListSync_20230105.csv"
# MAGIC val df = spark.read.format("csv")
# MAGIC   .option("inferSchema", "true")
# MAGIC   .option("header", "true")
# MAGIC   .option("sep", "|")
# MAGIC   .load(file_location)
# MAGIC df.show()
# MAGIC display(df)
