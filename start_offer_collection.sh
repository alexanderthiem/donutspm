#!/bin/bash
screen -dmS collect_offers bash -c '
  cd /root/donutspm &&
  source donutspm/bin/activate &&
  export PYTHONASYNCIODEBUG=1
  python3 -u -m store_all_offers 2>&1 | tee collect_offers.log
'

