#!/bin/bash

# Script calls sudo itself so it should not be run as root
if [ "$EUID" == 0 ]; then
    echo "Do not run this script as root!"
    exit 1
fi

arg_new=0
arg_packages=0
arg_venv=0
arg_sensors=0
arg_processor=0

for arg in "$@"; do
    if [[ "$arg" == "--new" ]]; then
        arg_new=1
    elif [[ "$arg" == "--packages" ]]; then
        arg_packages=1
    elif [[ "$arg" == "--venv" ]]; then
        arg_venv=1
    elif [[ "$arg" == "--sensors" ]]; then
        arg_sensors=1
    elif [[ "$arg" == "--processor" ]]; then
        arg_processor=1
    fi
done

#
# Set up Environment
#
THIS_HOSTNAME=$(hostname)
THIS_LOCATION="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd $THIS_LOCATION
source .env

#
# Create DIRS
#
if [[ $arg_new -eq 1 ]]; then
    mkdir -p $PAROS_DATA_LOCATION
fi

#
# Install APT Packages
#
if [[ $arg_new -eq 1 ]] || [[ $arg_packages -eq 1 ]]; then
    sudo apt install python3-venv python3-dev prometheus-node-exporter
fi

#
# Python Setup
#
if [[ $arg_new -eq 1 ]] || [[ $arg_venv -eq 1 ]]; then
    python3 -m venv $PAROS_VENV_LOCATION
    source $PAROS_VENV_LOCATION/bin/activate
    pip install -r requirements.txt
fi

#
# Sensor Daemons
#
if [[ $arg_new -eq 1 ]] || [[ $arg_sensors -eq 1 ]]; then
    jq -c '.sensors[]' sensor_configs/$THIS_HOSTNAME.json | while read -r sensor; do
        driver=$(echo "$sensor" | jq -r '.driver')
        sensor_id=$(echo "$sensor" | jq -r '.sensor_id')
        args=$(echo "$sensor" | jq -r '.args')

        sudo tee /etc/systemd/system/paros-sampler-$sensor_id.service > /dev/null << EOF
[Unit]
Description=Paros Sampler $sensor_id
After=network-online.target,time-sync.target
Wants=network-online.target,time-sync.target

[Service]
WorkingDirectory=$THIS_LOCATION
ExecStart=$PAROS_VENV_LOCATION/bin/python $THIS_LOCATION/paros_sensors/$driver $sensor_id $args
Restart=always
RestartSec=10
User=pi

[Install]
WantedBy=multi-user.target
EOF

        sudo systemctl daemon-reload
        sudo systemctl enable paros-sampler-$sensor_id.service
    done
fi

#
# Processor Daemon
#
if [[ $arg_new -eq 1 ]] || [[ $arg_processor -eq 1 ]]; then
    sudo sudo tee /etc/systemd/system/paros-processor.service > /dev/null << EOF
[Unit]
Description=Paros Processor
After=network-online.target,time-sync.target
Wants=network-online.target,time-sync.target

[Service]
WorkingDirectory=$THIS_LOCATION
ExecStart=$PAROS_VENV_LOCATION/bin/python $THIS_LOCATION/processor.py
Restart=always
RestartSec=10
User=pi

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable paros-processor.service

    echo "DONE. Reboot node!"
fi
