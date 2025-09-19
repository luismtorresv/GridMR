# SubmitJobRequest


## Properties

| Name         | Type    | Description | Notes      |
| ------------ | ------- | ----------- | ---------- |
| **data_url** | **str** |             |
| **code_url** | **str** |             |
| **job_name** | **str** |             | [optional] |

## Example

```python
from openapi_client.models.submit_job_request import SubmitJobRequest

# TODO update the JSON string below
json = "{}"
# create an instance of SubmitJobRequest from a JSON string
submit_job_request_instance = SubmitJobRequest.from_json(json)
# print the JSON string representation of the object
print(SubmitJobRequest.to_json())

# convert the object into a dict
submit_job_request_dict = submit_job_request_instance.to_dict()
# create an instance of SubmitJobRequest from a dict
submit_job_request_from_dict = SubmitJobRequest.from_dict(submit_job_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
