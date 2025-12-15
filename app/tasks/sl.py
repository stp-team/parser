import logging

from stp_database.models.Stats.sl import SL

from app.api.sl import SlAPI
from app.core.db import get_stats_session

logger = logging.getLogger(__name__)


async def fill_sl(api: SlAPI) -> None:
    """Получает значения SL и заполняет таблицы.

    Args:
        api: Экземпляр API
    """
    queues_obj = await api.get_vq_chat_filter()
    queue_list = [vq for queue in queues_obj.ntp_nck.queues for vq in queue.vqList]

    sl = await api.get_sl(
        start_date="14.12.2025",
        stop_date="15.12.2025",
        units=[7],
        queues=queue_list,
    )

    # Initialize SL object
    sl_object = SL()

    # Fill SL model from totalData
    for data in sl.totalData:
        # Map the data based on text field
        if data.text == "Поступило":
            sl_object.received_contacts = int(data.value)
        elif data.text == "Принято":
            sl_object.accepted_contacts = int(data.value)
        elif data.text == "Пропущено":
            sl_object.missed_contacts = int(data.value)
        elif data.text == "Принято в SL":
            sl_object.sl_contacts = int(data.value)
        elif data.text == "% Принятых":
            sl_object.accepted_contacts_percent = float(data.value)
        elif data.text == "% Пропущенных":
            sl_object.missed_contacts_percent = float(data.value)
        elif data.text == "SL":
            sl_object.sl = float(data.value)
        elif data.text == "Ср. время обр.":
            sl_object.average_proc_time = int(data.value)

    async with get_stats_session() as session:
        session.add(sl_object)
        await session.commit()
