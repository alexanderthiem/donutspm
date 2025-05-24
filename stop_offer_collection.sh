#!/bin/bash

# Try clean quit via Ctrl+C
screen -S collect_offers -X stuff $'\003'

# Give it 3 seconds to shut down
sleep 3

# Kill if it's still alive
screen -S collect_offers -X quit


