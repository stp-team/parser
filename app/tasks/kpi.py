async def fill_kpi():
    await _fill_day()
    await _fill_week()
    await _fill_month()


async def _fill_day(): ...


async def _fill_week(): ...


async def _fill_month(): ...
