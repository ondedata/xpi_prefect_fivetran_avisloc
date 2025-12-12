from prefect import flow, get_run_logger
from prefect_fivetran import FivetranCredentials
from prefect_fivetran.connectors import start_fivetran_connector_sync, wait_for_fivetran_connector_sync

@flow
def sync_avis_locataire(
    connector_id: str = "plug_reanalyze",
    credentials_block_name: str = "xpi-fivetran-test",
    poll_status_every_n_seconds: int = 5,
):
    logger = get_run_logger()
    logger.info("Loading Fivetran credentials block %s", credentials_block_name)
    fivetran_credentials = FivetranCredentials.load(credentials_block_name)

    last_sync = start_fivetran_connector_sync(
        connector_id=connector_id,
        fivetran_credentials=fivetran_credentials,
    )

    logger.info("Waiting for Fivetran connector %s to finish syncing", connector_id)
    sync_result = wait_for_fivetran_connector_sync(
        connector_id=connector_id,
        fivetran_credentials=fivetran_credentials,
        previous_completed_at=last_sync,
        poll_status_every_n_seconds=poll_status_every_n_seconds,
    )
    logger.info("Fivetran sync finished for connector %s", connector_id)
    return sync_result
if __name__ == "__main__":
    sync_avis_locataire()
