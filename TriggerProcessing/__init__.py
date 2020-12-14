import logging

import azure.functions as func


def main(req: func.HttpRequest, msg: func.Out[func.QueueMessage]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
        document_list = req_body.get('documentList')
    except ValueError:
        return func.HttpResponse(
             "Please pass a documentList parameter in the request body",
             status_code=400
        )
    
    if document_list:

        try:
            msg.set(document_list)
            return func.HttpResponse(
                "Processing started.",
                status_code=200
            )

        except Exception as e:
            return func.HttpResponse(
                f"Error: {e}",
                status_code=500
            )