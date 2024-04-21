import copy
import json
import sys
import time

one = {
  "pt_d": "20240418",
  "pt_h": "15",
  "pt_m": "00",
  "mediaid": "1719541979307180032",
  "adunitid": "1719542217245851648",
  "advertiserid": "1696818971278311424",
  "advertisertype": "0",
  "adgroupid": "1734986909684334592",
  "dsp_ad_exchange_sum_drn": 0,
  "dsp_ad_exchange_sum_drpn": 0,
  "dsp_ad_tracking_sum_dbwn": 12,
  "dsp_ad_exchange_avg_dbp": 0,
  "dsp_ad_tracking_avg_dbwp": 0.0,
  "dsp_ad_tracking_sum_din": 4,
  "dsp_ad_tracking_sum_dcn": 0,
  "dsp_ad_tracking_sum_ddn": 0,
  "dsp_ad_tracking_sum_all_dbn": 0.0,
  "dsp_ad_tracking_sum_dbn": 0.00591,
  "zt_ad_mixranking_sum_rn": 0,
  "zt_ad_mixranking_sum_rtan": 0.0,
  "zt_ad_ranking_sum_rbwn": 0,
  "zt_ad_mixranking_avg_rbp": 0.0,
  "zt_ad_ranking_avg_rbwp": 0.0,
  "zt_ad_tracking_sum_dyin": 0,
  "zt_ad_tracking_sum_dycn": 0,
  "zt_ad_tracking_sum_dydn": 0,
  "zt_ad_tracking_sum_dybn": 0.0,
  "dsp_ad_tracking_sum_all_dybn": 0.0,
  "ad_exchange_sum_drf": 0,
  "ad_exchange_sum_drpf": 0,
  "ad_exchange_sum_darpn": 0,
  "ad_mixranking_sum_f": 0,
  "ad_mixranking_sum_fn": 0,
  "ad_ranking_sum_dn": 1,
  "ad_exchange_sum_sdbp": 0.0,
  "ad_exchange_sum_secpm": 10,
  "ad_ranking_sum_cecpm": 13,
  "ad_ranking_sum_cdbwp": 12,
  "ad_ranking_sum_sdbwp": 10,
  "ad_tracking_sum_ddn": 0,
  "ad_tracking_sum_dbn": 0.00591
}

if __name__ == '__main__':
  if len(sys.argv) < 2:
    print("Usage: python gen.py <file>")
    sys.exit()
  current_time = int(time.time()) / 60 * 60
  time_str = str(time.strftime('%Y%m%d%H:%M:%S', time.localtime(current_time)))
  f = open(sys.argv[1], 'w')
  count = 0
  for i in range(5000):
    if count > 0:
      f.write("\n")
    count = count + 1
    mediaid = 1719541979307180032 + count
    add_one = copy.deepcopy(one)
    add_one['mediaid'] = mediaid
    add_one['pt_d'] = time_str[:8]
    add_one['pt_h'] = time_str[8:10]
    add_one['pt_m'] = time_str[11:13]
    add_one['ts'] = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time + count % 60)))
    f.write(json.dumps(add_one))
  f.close()
