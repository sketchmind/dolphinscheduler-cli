from __future__ import annotations

from ._base import BaseRequestsClient

from pydantic import TypeAdapter

from ...dao.entities.schedule import Schedule
from ..contracts.page_info import PageInfoSchedule
from ..contracts.schedule.schedule_create_request import ScheduleCreateRequest
from ..contracts.schedule.schedule_filter_request import ScheduleFilterRequest
from ..contracts.schedule.schedule_update_request import ScheduleUpdateRequest

class ScheduleV2Operations(BaseRequestsClient):
    def create_schedule(
        self,
        schedule_create_request: ScheduleCreateRequest
    ) -> Schedule:
        """
        Create
        
        Create resource schedule
        
        DS operation: ScheduleV2Controller.createSchedule | POST /v2/schedules
        
        Args:
            schedule_create_request: Request body payload.
        
        Returns:
            ResourceResponse object created
        """
        payload = self._request(
            "POST",
            "v2/schedules",
        json=self._json_payload(schedule_create_request),
        )
        return self._validate_payload(payload, TypeAdapter(Schedule))

    def filter_schedule(
        self,
        schedule_filter_request: ScheduleFilterRequest
    ) -> PageInfoSchedule:
        """
        Get
        
        Get resource schedule according to query parameter
        
        DS operation: ScheduleV2Controller.filterSchedule | POST /v2/schedules/filter
        
        Args:
            schedule_filter_request: Request body payload.
        
        Returns:
            result Result
        """
        payload = self._request(
            "POST",
            "v2/schedules/filter",
        json=self._json_payload(schedule_filter_request),
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoSchedule))

    def delete_schedule(
        self,
        id: int
    ) -> None:
        """
        Delete
        
        Delete schedule by id
        
        DS operation: ScheduleV2Controller.deleteSchedule | DELETE /v2/schedules/{id}
        
        Args:
            id: schedule object id
        """
        path = f"v2/schedules/{id}"
        self._request("DELETE", path)
        return None

    def get_schedule(
        self,
        id: int
    ) -> Schedule:
        """
        Get
        
        Get resource schedule by id
        
        DS operation: ScheduleV2Controller.getSchedule | GET /v2/schedules/{id}
        
        Args:
            id: schedule object id
        
        Returns:
            result Result
        """
        path = f"v2/schedules/{id}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(Schedule))

    def update_schedule(
        self,
        id: int,
        schedule_update_request: ScheduleUpdateRequest
    ) -> Schedule:
        """
        Update
        
        Update resource schedule
        
        DS operation: ScheduleV2Controller.updateSchedule | PUT /v2/schedules/{id}
        
        Args:
            id: schedule object id
            schedule_update_request: Request body payload.
        
        Returns:
            result Result
        """
        path = f"v2/schedules/{id}"
        payload = self._request(
            "PUT",
            path,
        json=self._json_payload(schedule_update_request),
        )
        return self._validate_payload(payload, TypeAdapter(Schedule))

__all__ = ["ScheduleV2Operations"]
