from prefect import flow, task
from prefect_fivetran import FivetranCredentials
from prefect_fivetran.connectors import start_fivetran_connector_sync, wait_for_fivetran_connector_sync
from prefect_fivetran import credentials

@flow
def sync_avis_locataire():
    fivetran_credentials = FivetranCredentials.load("xpi-fivetran-test")

    last_sync = start_fivetran_connector_sync(
        connector_id="plug_reanalyze",
        fivetran_credentials=fivetran_credentials,
    )

    return wait_for_fivetran_connector_sync(
        connector_id="plug_reanalyze",
        fivetran_credentials=fivetran_credentials,
        previous_completed_at=last_sync,
        poll_status_every_n_seconds=5,
    )
if __name__ == "__main__":
    sync_avis_locataire()
