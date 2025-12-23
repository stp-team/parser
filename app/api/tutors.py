import logging

from app.api.base import BaseAPI
from app.models.tutors import GraphFiltersResponse, TutorGraphResponse


class TutorsAPI(BaseAPI):
    def __init__(self, session):
        super().__init__(session)
        self.service_url = "tutor-graph/tutor-api"
        self.logger = logging.getLogger(self.__class__.__name__)

    async def get_full_graph(
        self,
        division_id: int,
        start_date: str,
        stop_date: str,
        picked_units: list[int],
        picked_tutor_types: list[int],
        picked_shift_types: list[int],
        tz: int = 0,
    ) -> TutorGraphResponse | None:
        """
        Get full graph data for tutors.

        Args:
            division_id: Division ID
            start_date: Start date in DD.MM.YYYY format
            stop_date: Stop date in DD.MM.YYYY format
            picked_units: List of unit IDs
            picked_tutor_types: List of tutor type IDs
            picked_shift_types: List of shift type IDs
            tz: Timezone offset (default: 0)
        """
        # Prepare form data as a list of tuples to handle multiple values for the same key
        form_data = [
            ("tz", str(tz)),
            ("divisionId", str(division_id)),
            ("startDate", start_date),
            ("stopDate", stop_date),
        ]

        # Add array parameters
        for unit in picked_units:
            form_data.append(("pickedUnits[]", str(unit)))
        for tutor_type in picked_tutor_types:
            form_data.append(("pickedTutorTypes[]", str(tutor_type)))
        for shift_type in picked_shift_types:
            form_data.append(("pickedShiftTypes[]", str(shift_type)))

        # Override headers for form data
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        response = await self.post(
            f"{self.service_url}/get-full-graph", data=form_data, headers=headers
        )

        if response.status != 200:
            return None

        try:
            data = await response.json()
            tutor_graph = TutorGraphResponse.model_validate(data)
            return tutor_graph
        except Exception:
            return None

    async def get_graph_filters(self, division_id: int) -> GraphFiltersResponse | None:
        """
        Get graph filters data including all tutors, units, shift types, and tutor types.

        Returns:
            GraphFiltersResponse containing lists of tutors, units, shift types, and tutor types
        """
        form_data = [("divisionId", str(division_id))]

        # Override headers for form data
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        response = await self.post(
            f"{self.service_url}/get-graph-filters", data=form_data, headers=headers
        )

        if response.status != 200:
            return None

        try:
            data = await response.json()
            graph_filters = GraphFiltersResponse.model_validate(data)
            return graph_filters
        except Exception:
            return None
