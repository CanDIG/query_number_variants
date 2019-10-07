# Python 3 only

import requests
import json
import datetime
import os
from requests_futures.sessions import FuturesSession

##################### load configs ########################
###########################################################

try:
	with open('query_number_variants_options.json') as json_file:
		config = json.load(json_file)
		start = config["start"]
		end = config["end"]
		reference_name = config["reference_name"]
		increment = config["increment"]
		server_address = config["server_address"]
		ethnicities = config["ethnicities"]
		dataset_id = config["dataset_id"]
		async_worker = config["async_workers"]

except Exception:
	print("Something is wrong with your config file, make sure it contains async_worker, \
		start, end, referenceName, increment, server_address, ethnicities and dataset_id")

##################initialize output dir#####################

timestamp_path = 'output_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

try:
	os.mkdir(timestamp_path)

	# init log file
	with open(timestamp_path + '/log.csv', 'a', encoding='utf-8') as file:
		file.write('population,start,end,count\n')


except OSError:
	print("path creation failed")

###########################################################
total_result = {}
total_result["config"] = {}
total_result["results"] = {}
total_result["config"]["reference_name"] = reference_name
total_result["config"]["start"] = start
total_result["config"]["end"] = end
total_result["config"]["dataset_id"] = dataset_id
total_result["config"]["increment"] = increment


def generate_model_request(ethnicity, curr_start, curr_end):
	return {
			"dataset_id": dataset_id,
			"logic": {
				"id": "A"
			},
			"components": [
				{
					"id": "A",
					"patients": {
						"filters": [
							{
								"field": "ethnicity",
								"operator": "==",
								"value": ethnicity
							}
						]
					}
				}
			],
			"results": [
				{
					"table": "variants",
					"referenceName": reference_name,
					"start": str(curr_start),
					"end": str(curr_end),
					"fields": ["start", "end", "id", "variantSetId"]
				}
			]
	}

def construct_async_request_queues(ethnicity, curr_start, curr_end):

	requests_queue = []

	# The following while loop constructs a complete request queue
	while curr_end <= end:
		model_request = generate_model_request(ethnicity, curr_start, curr_end)

		requests_queue.append(model_request)

		curr_start = curr_end + 1

		# When the curr_end is the same as end, break out of the while loop
		if curr_end == end:
			break

		if curr_end + increment > end:
			curr_end = end
		else:
			curr_end = curr_end + increment

	return requests_queue


def output_stats_for_current_ethnicity(ethnicity, total):

	output_dict = {}
	output_dict["ethnicity"] = ethnicity
	output_dict["start"] = start
	output_dict["end"] = end

	output_file_name = ethnicity + '_chr' + reference_name + '_' + str(start) + '_' + str(end) + '.json'
	
	with open(timestamp_path + '/' + output_file_name, 'w') as f:
		output_dict['total'] = total

		json.dump(output_dict, f, indent=4)


def write_to_log(content):

	with open(timestamp_path + '/log.csv', 'a', encoding='utf-8') as file:
		file.write(content)

def deduplicate_count(prev_curr_res):
	temp_merge = prev_curr_res['prev'] + prev_curr_res['curr']
	deduplicate_temp_merge = [dict(t) for t in {tuple(d.items()) for d in temp_merge}]
	
	return len(prev_curr_res['curr']) - len(temp_merge) + len(deduplicate_temp_merge)


def main():
	for ethnicity in ethnicities:
		print("Following logs are for", ethnicity)

		# initialize
		total = 0
		curr_start = start
		curr_end = curr_start + increment
		header = {'Content-Type': 'Application/json'}

		# If after the increment, the curr_end is bigger than the end, overwrite it to end
		if curr_end > end:
			curr_end = end

		# Construct request queues

		requests_queue = construct_async_request_queues(ethnicity, curr_start, curr_end)

		print("Async requests queue for", ethnicity, "contains", len(requests_queue), "requests")

		async_session = FuturesSession(max_workers=async_worker)

		responses = [async_session.post(server_address, json=request, headers=header) for request in requests_queue]

		prev_curr_res = {}

		for idx, future_response in enumerate(responses):
			curr_req = requests_queue[idx]['results'][0]

			try:
				response = future_response.result()
				res = response.json()

				# The first iteration, use the count as it is; and store the result as prev
				if prev_curr_res.get('prev') is None:
					prev_curr_res['prev'] = res['results']['variants']
					dedup_count = res['results']['total']
				else:
					prev_curr_res['curr'] = res['results']['variants']
					dedup_count = deduplicate_count(prev_curr_res)

					# Rewrite response of current iteration to prev
					prev_curr_res['prev'] = res['results']['variants']

				total = total + dedup_count

				print(ethnicity, 'contains', dedup_count, 'variants from range', curr_req['start'], curr_req['end'])
				write_to_log(ethnicity + "," + str(curr_req['start']) + ',' + str(curr_req['end'] + ',' + str(dedup_count) + '\n'))

			except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
				print(ethnicity, 'ConnectionError occurs at', curr_req['start'], curr_req['end'])
				write_to_log(ethnicity + "," + str(curr_req['start']) + ',' + str(curr_req['end'] + ',conn_error\n'))

			except KeyError as e:
				print(ethnicity, 'KeyError occurs at', curr_req['start'], curr_req['end'])
				write_to_log(ethnicity + "," + str(curr_req['start']) + ',' + str(curr_req['end'] + ',key_error\n'))


		print('In total,', ethnicity, 'contains', total, 'variants', 'from range', start, end, 'in chr', reference_name)

		output_stats_for_current_ethnicity(ethnicity, total)
		total_result["results"][ethnicity] = total

	with open(timestamp_path + '/overview.json', 'w') as f:
		json.dump(total_result, f, indent=4)


main()
