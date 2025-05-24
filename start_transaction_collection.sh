#!/bin/bash
screen -dmS collect_transaction bash -c '
  cd /root/donutspm &&
  source donutspm/bin/activate &&
  export PYTHONASYNCIODEBUG=1
  python3 -u -m store_all_transactions 2>&1 | tee collect_transaction.log
'

