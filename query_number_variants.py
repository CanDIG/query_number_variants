# Python 3 only

import requests
import json
import datetime
import os

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

def main():
	for ethnicity in ethnicities:
		print("Following logs are for", ethnicity)

		total = 0
		curr_start = start
		curr_end = curr_start + increment

		# If after the increment, the curr_end is bigger than the end, overwrite it to end
		if curr_end > end:
			curr_end = end

		output_dict = {}
		temp_result_list = []
		output_dict["ethnicity"] = ethnicity
		output_dict["start"] = start
		output_dict["end"] = end

		while curr_end <= end:

			model_request = generate_model_request(ethnicity, curr_start, curr_end)

			r = requests.post(server_address, data=json.dumps(model_request), headers={'content-type':'application/json'})

			res = r.json()

			temp_result_list = temp_result_list + res['results']['variants']

			total = total + res['results']['total']

			print(ethnicity, 'contains', res['results']['total'], 'variants','from range', curr_start, curr_end)

			curr_start = curr_end + 1

			# When the curr_end is the same as end, break out of the while loop
			if curr_end == end:
				break

			if curr_end + increment > end:
				curr_end = end
			else:
				curr_end = curr_end + increment

		deduplicated_list = [dict(t) for t in {tuple(d.items()) for d in temp_result_list}]

		print('In total,', ethnicity, 'contains', total, 'variants', 'from range', start, end, 'in chr', reference_name)

		output_file_name = ethnicity + '_chr' + reference_name + '_' + str(start) + '_' + str(end) + '.json'

		total_result[ethnicity] = len(deduplicated_list)
		
		with open(timestamp_path + '/' + output_file_name, 'w') as f:
			output_dict['preliminary_total'] = total
			output_dict['total'] = len(deduplicated_list)
			output_dict['results'] = deduplicated_list

			json.dump(output_dict, f, indent=4)

	with open(timestamp_path + '/overview.json', 'w') as f:
		json.dump(total_result, f, indent=4)


main()