import requests

from app.api.sl import SlAPI


async def fill_sl(session: requests.Session):
    sl = SlAPI(session)
    queues_obj = await sl.get_vq_chat_filter()
    queue_list = [vq for queue in queues_obj.ntp_nck.queues for vq in queue.vqList]

    sl = await sl.get_sl(
        start_date="09.12.2025",
        stop_date="10.12.2025",
        units=[7],
        queues=queue_list,
    )

    print(sl)
