import logging
import os
import azure.functions as func

from ..utils import storage_helpers
from ..utils import processing


def main(msg: func.QueueMessage) -> None:
    
    file_name = ""

    try:

        file_name = msg.get_body().decode('utf-8')
        logging.info(f"Processing queue item: {file_name}...")

        # Getting settings
        STORAGE_CONNECTION_STRING = os.getenv("QueueConnectionString")
        CONTAINER_NAME = os.getenv("STORAGE_CONTAINER_NAME")
        TABLE_NAME = os.getenv("STORAGE_TABLE_NAME")

        # Updating status to new
        storage_helpers.update_status(TABLE_NAME, file_name, 'new', STORAGE_CONNECTION_STRING)

        # Getting file from storage
        file_path = storage_helpers.download_blob(CONTAINER_NAME, file_name, STORAGE_CONNECTION_STRING)

        if file_path != None:
            # Processing file
            processed_doc = processing.process_doc(file_path)
            # Saving processed file to storage
            if processed_doc != None:
                # Updating status to processed
                storage_helpers.update_status(TABLE_NAME, file_name, 'processed', STORAGE_CONNECTION_STRING)
                new_file_name = 'processed_' + file_name
                storage_helpers.upload_blob(CONTAINER_NAME, new_file_name, processed_doc, STORAGE_CONNECTION_STRING)
                # Updating status to done
                storage_helpers.update_status(TABLE_NAME, file_name, 'done', STORAGE_CONNECTION_STRING)
                # Deleting local copy
                os.remove(file_path)
                logging.info(f"Done processing {file_name}.")
        
        else:
            logging.info(f"Did not perform any operation as there was an issue.")
            # Updating status to failed
            storage_helpers.update_status(TABLE_NAME, file_name, 'failed', STORAGE_CONNECTION_STRING)

    except Exception as e:
        logging.error(f"Error getting file name from msg: {e}")
