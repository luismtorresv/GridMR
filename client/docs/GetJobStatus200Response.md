# GetJobStatus200Response


## Properties

| Name         | Type      | Description | Notes      |
| ------------ | --------- | ----------- | ---------- |
| **job_id**   | **str**   |             | [optional] |
| **status**   | **str**   |             | [optional] |
| **progress** | **float** |             | [optional] |

## Example

```python
from openapi_client.models.get_job_status200_response import GetJobStatus200Response

# TODO update the JSON string below
json = "{}"
# create an instance of GetJobStatus200Response from a JSON string
get_job_status200_response_instance = GetJobStatus200Response.from_json(json)
# print the JSON string representation of the object
print(GetJobStatus200Response.to_json())

# convert the object into a dict
get_job_status200_response_dict = get_job_status200_response_instance.to_dict()
# create an instance of GetJobStatus200Response from a dict
get_job_status200_response_from_dict = GetJobStatus200Response.from_dict(get_job_status200_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
