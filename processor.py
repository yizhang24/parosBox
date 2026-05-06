import influxdb_client
import pathlib
from dotenv import load_dotenv
import os
import datetime
import pickle
import socket
import json
import logging
from time import sleep
import re

class parosProcessor:

    POINTER_PATH = 'pointer.pickle'
    MAXIMUM_UPLOAD_SIZE = 600  # Maximum # of lines/datapoints for each upload
    LOOP_PERIOD = 1  # Loop timing control period

    def __init__(self, data_loc, influx_host, influx_org, influx_bucket, influx_token):
        #
        # Instance Vars
        #

        # Parameters
        self.data_loc = data_loc
        self.influx_bucket = influx_bucket
        self.influx_fail = False
        self.hostname = socket.gethostname()

        # InfluxDB Objects
        self.influx_client = influxdb_client.InfluxDBClient(
            url=influx_host,
            token=influx_token,
            org=influx_org
        )
        self.influx_write_api = self.influx_client.write_api(write_options=influxdb_client.client.write_api.SYNCHRONOUS, debug=True)

        # List of Sensors
        self.sensors = []
        with open(f'sensor_configs/{self.hostname}.json', 'r') as f:
            # load this box's sensor json file and parse the sensor_id
            sensors_json = json.load(f)['sensors']
            for sensor in sensors_json:
                self.sensors.append(sensor['sensor_id'])
                logging.debug(f"Found sensor {sensor}")

        #
        # Pointer File Creation
        #
        cur_time = datetime.datetime.now(datetime.UTC)
        file_hour = cur_time.strftime('%Y-%m-%d-%H')

        for sensor in self.sensors:
            cur_pointer = self.getPointer(sensor)
            if cur_pointer is None:
                logging.info(f"Adding new sensor {sensor} to pointer file")
                self.setPointer(sensor, file_hour, 0)

    def getPointer(self, sensor_id = None):
        if os.path.isfile(self.POINTER_PATH) and os.path.getsize(self.POINTER_PATH) > 0:
            # Only try to open the file if it exists and its size is greather than 0
            with open(self.POINTER_PATH, 'rb') as f:
                # pickle files are opened in binary mode
                cur_pointer = pickle.load(f)
                # If a sensor_id is requested, send only that. Otherwise, send the whole dict
                if sensor_id is None:
                    return cur_pointer
                else:
                    if sensor_id in cur_pointer:
                        return cur_pointer[sensor_id]
                    else:
                        return None
        else:
            # return none if the file doesn't exist
            return None

    def setPointer(self, sensor_id, hour, offset):
        # get the current pointer to update its value
        cur_pointer = self.getPointer()
        if cur_pointer is None:
            cur_pointer = {}

        cur_pointer[sensor_id] = [hour, offset]
        with open(self.POINTER_PATH, 'wb') as f:
            # overwrite existing pickle file in binary mode
            pickle.dump(cur_pointer, f)
            logging.debug(f"Updated pointer file for sensor {sensor_id} with values hour={hour} and offset={offset}")

    def __getLatestData(self, cur_path, cur_offset):
        with open(cur_path, 'rb') as f:
            # Open indicated data file and seek to pointer offset
            # Storing the offset is much faster than reading the
            # whole file every time
            f.seek(cur_offset)

            output_str = ""  # stored line protocols that are new
            line_counter = 0  # stores the number of lines added

            # Do not allow a single block of more than the number of lines
            # specified in self.MAXIMUM_UPLOAD_SIZE
            while line_counter <= self.MAXIMUM_UPLOAD_SIZE:
                lp_str = f.readline().rstrip(b'\x00')
                if not lp_str:
                    # Arrived at the end of the file
                    break

                # Validate lp_str and revert if needed
                while not lp_str.startswith(self.hostname.encode()):
                    # That's an issue! Go back one byte until the string is valid
                    cur_offset -= 1
                    f.seek(cur_offset)
                    lp_str = f.readline()

                output_str += lp_str.decode()  # append line to output
                line_counter += 1  # incremement line counter
                cur_offset += len(lp_str)  # update offset by the length of the line

        return output_str,cur_offset,line_counter

    def __processSensor(self, sensor):
        cur_sensor_dir = os.path.join(self.data_loc, sensor)  # Find the sensor data path in the filesystem
        cur_file,cur_offset = self.getPointer(sensor)  # Get the state of the current pointer for this sensor
        cur_path = os.path.join(cur_sensor_dir, cur_file)  # Get full path of the current data file

        # this stored the output line-protocol for the given sensors during this loop
        output_lp = ""
        cur_pointer_time = datetime.datetime.strptime(cur_file, '%Y-%m-%d-%H')  # Create a datetime object from the stored hour

        num_lines = 0  # initialize num_lines var for later

        if os.path.isfile(cur_path):
            # This is where the data is actually pulled from the file, only if the file exists
            output_lp,cur_offset,num_lines = self.__getLatestData(cur_path, cur_offset)

        if output_lp:
            # There is new line protocol to send to InfluxDB
            try:
                # Send 'em off!
                self.influx_write_api.write(
                    bucket = self.influx_bucket,
                    record = output_lp
                )

                if self.influx_fail:
                    logging.info("Conenction to InfluxDB restored")
                    self.influx_fail = False

                logging.debug(f"Uploaded {num_lines} of line-protocol for sensor {sensor}")

                # Update the pointer ONLY after successfully sending to InfluxDB
                self.setPointer(sensor, cur_file, cur_offset)
            except Exception as e:
                if not self.influx_fail:
                    logging.error(f"Connection to InfluxDB Lost: {e}")
                    self.influx_fail = True

                pass
        else:
            # Nothing new to send
            # This will execute if the program is running too fast (not an issue)
            # or if the file is no longer being written to. In this case usually
            # it is time to switch to the next hour of data. This also allows the processor
            # to "find" the next available file if the program is running behind without
            # having to list the whole directory of files and sort them, which takes
            # a long time
            if self.__getHourOnlyUTCNow() > cur_pointer_time:
                # Verify that the sensors aren't time traveling before
                # switching to the new file
                cur_pointer_time += datetime.timedelta(hours=1)
                cur_file = cur_pointer_time.strftime('%Y-%m-%d-%H')
                cur_offset = 0

                self.setPointer(sensor, cur_file, cur_offset)

        # Returns the number of lines uploaded to influxdb
        return num_lines

    def __getHourOnlyUTCNow(self):
        # Gets the current datetime in UTC then removes timezone info, and removes
        # anything more granular than an hour for comparison purposes
        return datetime.datetime.now(datetime.UTC).replace(tzinfo=None, minute=0, second=0, microsecond=0)

    def processorLoop(self):
        # Main loop
        while True:
            try:
                # Record system time when starting an iteration
                loop_start_time = datetime.datetime.now()
                max_num_lines = 0  # stores the maximum numnber of lines sent during this iteration

                # loop through each sensor and poll files
                for sensor in self.sensors:
                    cur_num_lines = self.__processSensor(sensor)

                    # if this is a maximum, update
                    if cur_num_lines > max_num_lines:
                        max_num_lines = cur_num_lines

                # Timing control portion of the loop. Usually, there is no reason for the program to
                # be looping as fast as possible, so we wait until the current system time is at least
                # 1 second past the time when the iteration started. The exception is that if the program
                # needs to catch up, which is evident by the lines being sent being equal to maximum upload,
                # then we want the program to keep looping without control until it is stable again
                if max_num_lines < self.MAXIMUM_UPLOAD_SIZE:
                    while datetime.datetime.now() < loop_start_time + datetime.timedelta(seconds=self.LOOP_PERIOD):
                        sleep(0.01)

            except KeyboardInterrupt:
                # Handles ctrl+c events
                logging.info("Stopping processor from key interrupt")
                exit(0)

def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # Read .env file
    file_path = pathlib.Path(__file__).parent.resolve()
    load_dotenv(f"{file_path}/.env")

    # Defined required env variables
    required_envs = [
        "PAROS_DATA_LOCATION",
        "PAROS_INFLUXDB_HOST",
        "PAROS_INFLUXDB_ORG",
        "PAROS_INFLUXDB_BUCKET",
        "PAROS_INFLUXDB_TOKEN"
    ]

    for env_item in required_envs:
        if os.getenv(env_item) is None:
            logging.critical(f"Unable to find environment variable {env_item}. Does .env exist?")
            exit(1)

    # Create processor
    processor = parosProcessor(
        os.getenv("PAROS_DATA_LOCATION"),
        os.getenv("PAROS_INFLUXDB_HOST"),
        os.getenv("PAROS_INFLUXDB_ORG"),
        os.getenv("PAROS_INFLUXDB_BUCKET"),
        os.getenv("PAROS_INFLUXDB_TOKEN")
    )

    # Main loop in the main thread
    logging.info("Starting processing loop...")
    processor.processorLoop()

if __name__ == "__main__":
    main()
