from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.project_preference import ProjectPreference

class EnableProjectPreferenceParams(BaseParamsModel):
    """
    Enable Project Preference
    
    Form parameters for ProjectPreferenceController.enableProjectPreference.
    """
    state: int

class UpdateProjectPreferenceParams(BaseParamsModel):
    """
    Update Project Preference
    
    Form parameters for ProjectPreferenceController.updateProjectPreference.
    """
    projectPreferences: str

class ProjectPreferenceOperations(BaseRequestsClient):
    def query_project_preference_by_project_code(
        self,
        project_code: int
    ) -> ProjectPreference:
        """
        Query Project Preference By Project Code
        
        DS operation: ProjectPreferenceController.queryProjectPreferenceByProjectCode | GET /projects/{projectCode}/project-preference
        """
        path = f"projects/{project_code}/project-preference"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(ProjectPreference))

    def enable_project_preference(
        self,
        project_code: int,
        form: EnableProjectPreferenceParams
    ) -> None:
        """
        Enable Project Preference
        
        DS operation: ProjectPreferenceController.enableProjectPreference | POST /projects/{projectCode}/project-preference
        
        Args:
            form: Form parameters bag for this operation.
        """
        path = f"projects/{project_code}/project-preference"
        data = self._model_mapping(form)
        self._request(
            "POST",
            path,
        data=data,
        )
        return None

    def update_project_preference(
        self,
        project_code: int,
        form: UpdateProjectPreferenceParams
    ) -> ProjectPreference:
        """
        Update Project Preference
        
        DS operation: ProjectPreferenceController.updateProjectPreference | PUT /projects/{projectCode}/project-preference
        
        Args:
            form: Form parameters bag for this operation.
        """
        path = f"projects/{project_code}/project-preference"
        data = self._model_mapping(form)
        payload = self._request(
            "PUT",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(ProjectPreference))

__all__ = ["ProjectPreferenceOperations"]
