import client.openapi_client as openapi_client
from client.openapi_client.models import SubmitJobRequest

configuration = openapi_client.Configuration(host="http://localhost:8000")

with openapi_client.ApiClient(configuration) as api_client:
    api_instance = openapi_client.DefaultApi(api_client)
    job_id = "3"
    try:
        api_response = api_instance.submit_job(
            submit_job_request=SubmitJobRequest(
                code_url="file://192.168.0.1/code",
                data_url="file://192.168.0.1/file",
                job_name="Just testing it out",
            )
        )
        print(api_response)
    except openapi_client.rest.ApiException as e:
        print(e)
