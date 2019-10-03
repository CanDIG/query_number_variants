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
except Exception:
	print("Something is wrong with your config file, make sure it contains \
		start, end, referenceName, increment, server_address, ethnicities and dataset_id")

##################initialize output dir#####################

timestamp_path = 'output_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

try:
	os.mkdir(timestamp_path)
except OSError:
	print("path creation failed")

###########################################################
total_result = {}
total_result["reference_name"] = reference_name
total_result["start"] = start
total_result["end"] = end
total_result["dataset_id"] = dataset_id
total_result["increment"] = increment


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


def output_stats_for_current_ethnicity(temp_result_list, ethnicity, total):

	output_dict = {}
	output_dict["ethnicity"] = ethnicity
	output_dict["start"] = start
	output_dict["end"] = end

	deduplicated_list = [dict(t) for t in {tuple(d.items()) for d in temp_result_list}]

	output_file_name = ethnicity + '_chr' + reference_name + '_' + str(start) + '_' + str(end) + '.json'

	total_result[ethnicity] = len(deduplicated_list)
	
	with open(timestamp_path + '/' + output_file_name, 'w') as f:
		output_dict['preliminary_total'] = total
		output_dict['total'] = len(deduplicated_list)
		output_dict['results'] = deduplicated_list

		json.dump(output_dict, f, indent=4)


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

		async_session = FuturesSession(max_workers=5)

		responses = [async_session.post(server_address, json=request, headers=header) for request in requests_queue]

		temp_result_list = []

		for idx, future_response in enumerate(responses):
			try:
				response = future_response.result()

				res = response.json()
				temp_result_list = temp_result_list + res['results']['variants']
				total = total + res['results']['total']

				print(ethnicity, 'contains', res['results']['total'], 'variants','from range', requests_queue[idx]['results'][0]['start'], requests_queue[idx]['results'][0]['end'])
			except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
				print("some sort of connection error")

		print('In total,', ethnicity, 'contains', total, 'variants', 'from range', start, end, 'in chr', reference_name)

		output_stats_for_current_ethnicity(temp_result_list, ethnicity, total)

	with open(timestamp_path + '/overview.json', 'w') as f:
		json.dump(total_result, f, indent=4)


main()
