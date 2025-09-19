# SubmitJob201Response


## Properties

| Name       | Type    | Description | Notes      |
| ---------- | ------- | ----------- | ---------- |
| **job_id** | **str** |             | [optional] |
| **status** | **str** |             | [optional] |

## Example

```python
from openapi_client.models.submit_job201_response import SubmitJob201Response

# TODO update the JSON string below
json = "{}"
# create an instance of SubmitJob201Response from a JSON string
submit_job201_response_instance = SubmitJob201Response.from_json(json)
# print the JSON string representation of the object
print(SubmitJob201Response.to_json())

# convert the object into a dict
submit_job201_response_dict = submit_job201_response_instance.to_dict()
# create an instance of SubmitJob201Response from a dict
submit_job201_response_from_dict = SubmitJob201Response.from_dict(submit_job201_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
