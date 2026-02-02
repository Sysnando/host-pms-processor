"""Integration tests with mocked AWS and API services."""

import json
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from src.aws import S3Manager, SQSManager
from src.clients import ClimberESBClient, HostPMSAPIClient
from src.models.climber.reservation import ReservationCollection
from src.services import HostPMSConnectorOrchestrator


@pytest.fixture
def mock_s3_manager():
    """Create a mock S3Manager."""
    with patch("src.services.orchestration_service.S3Manager") as mock:
        manager = Mock()
        manager.upload_raw = Mock(
            return_value={"key": "raw/config.json", "url": "s3://bucket/raw/config.json"}
        )
        manager.upload_processed = Mock(
            return_value={
                "key": "processed/config.json",
                "url": "s3://bucket/processed/config.json",
            }
        )
        mock.return_value = manager
        yield manager


@pytest.fixture
def mock_sqs_manager():
    """Create a mock SQSManager."""
    with patch("src.services.orchestration_service.SQSManager") as mock:
        manager = Mock()
        manager.send_message = Mock(return_value={"message_id": "msg-12345"})
        mock.return_value = manager
        yield manager


@pytest.fixture
def mock_esb_client():
    """Create a mock ClimberESBClient."""
    with patch(
        "src.services.orchestration_service.ClimberESBClient"
    ) as mock:
        client = Mock()
        client.get_hotels = AsyncMock(
            return_value=[
                {"code": "HOTEL001", "name": "Hotel 1"},
                {"code": "HOTEL002", "name": "Hotel 2"},
            ]
        )
        client.get_hotel_parameters = AsyncMock(
            return_value={"lastImportDate": "2024-01-01T00:00:00Z"}
        )
        client.register_file = AsyncMock(return_value={})
        client.update_import_date = AsyncMock(return_value={})
        mock.return_value = client
        yield client


@pytest.fixture
def mock_host_api_client():
    """Create a mock HostPMSAPIClient."""
    with patch(
        "src.services.orchestration_service.HostPMSAPIClient"
    ) as mock:
        client = Mock()
        client.get_hotel_config = AsyncMock(
            return_value={
                "hotelCode": "HOTEL001",
                "hotelName": "Sample Hotel",
                "rooms": [{"code": "D", "name": "Double"}],
                "roomTypes": [],
                "segments": [],
            }
        )
        client.get_reservations = AsyncMock(
            return_value={
                "reservations": [
                    {
                        "reservationId": "RES001",
                        "hotelCode": "HOTEL001",
                        "status": "ACTIVE",
                        "roomStays": [],
                        "totalRevenue": 100.0,
                    }
                ]
            }
        )
        mock.return_value = client
        yield client


class TestS3ManagerIntegration:
    """Integration tests for S3Manager."""

    @patch("boto3.client")
    def test_upload_raw_success(self, mock_boto_client):
        """Test successful raw data upload."""
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3

        manager = S3Manager()
        result = manager.upload_raw(
            hotel_code="HOTEL001",
            data_type="hotel-configs",
            data={"test": "data"},
        )

        assert result["key"] == f"HOTEL001/hotel-configs-{result['key'].split('-')[1]}"
        assert "s3://" in result["url"]
        mock_s3.put_object.assert_called_once()

    @patch("boto3.client")
    def test_upload_processed_success(self, mock_boto_client):
        """Test successful processed data upload."""
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3

        manager = S3Manager()
        result = manager.upload_processed(
            hotel_code="HOTEL001",
            data_type="hotel-configs",
            data={"test": "data"},
        )

        assert result["key"] == f"HOTEL001/hotel-configs-{result['key'].split('-')[1]}"
        assert "s3://" in result["url"]
        mock_s3.put_object.assert_called_once()

    @patch("boto3.client")
    def test_list_objects_success(self, mock_boto_client):
        """Test successfully listing S3 objects."""
        mock_s3 = Mock()
        mock_paginator = Mock()
        mock_paginator.paginate = Mock(
            return_value=[
                {
                    "Contents": [
                        {
                            "Key": "HOTEL001/config.json",
                            "Size": 1024,
                            "LastModified": "2024-01-15T10:00:00Z",
                        }
                    ]
                }
            ]
        )
        mock_s3.get_paginator = Mock(return_value=mock_paginator)
        mock_boto_client.return_value = mock_s3

        manager = S3Manager()
        objects = manager.list_objects("test-bucket", "HOTEL001/")

        assert len(objects) == 1
        assert objects[0]["key"] == "HOTEL001/config.json"


class TestSQSManagerIntegration:
    """Integration tests for SQSManager."""

    @patch("boto3.client")
    def test_send_message_success(self, mock_boto_client):
        """Test successfully sending SQS message."""
        mock_sqs = Mock()
        mock_sqs.get_queue_url = Mock(
            return_value={"QueueUrl": "https://sqs.us-east-1.amazonaws.com/queue"}
        )
        mock_sqs.send_message = Mock(return_value={"MessageId": "msg-123"})
        mock_boto_client.return_value = mock_sqs

        manager = SQSManager()
        result = manager.send_message(
            hotel_code="HOTEL001",
            file_type="config",
            file_key="HOTEL001/config.json",
        )

        assert result["message_id"] == "msg-123"
        mock_sqs.send_message.assert_called_once()

        # Verify GroupId and MessageDeduplicationId are set (FIFO specific)
        call_args = mock_sqs.send_message.call_args
        assert call_args.kwargs["GroupId"] == "HOTEL001"
        assert "MessageDeduplicationId" in call_args.kwargs


class TestAPIClientIntegration:
    """Integration tests for API clients."""

    @pytest.mark.asyncio
    async def test_esb_client_get_hotels(self):
        """Test ESB client getting hotel list."""
        with patch("httpx.AsyncClient") as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json = Mock(
                return_value={
                    "hotels": [
                        {"code": "HOTEL001", "name": "Hotel 1"},
                        {"code": "HOTEL002", "name": "Hotel 2"},
                    ]
                }
            )

            mock_async_client = AsyncMock()
            mock_async_client.request = AsyncMock(return_value=mock_response)
            mock_async_client.__aenter__ = AsyncMock(return_value=mock_async_client)
            mock_async_client.__aexit__ = AsyncMock(return_value=None)

            mock_client.return_value = mock_async_client

            client = ClimberESBClient()
            hotels = await client.get_hotels()

            assert len(hotels) == 2
            assert hotels[0]["code"] == "HOTEL001"

    @pytest.mark.asyncio
    async def test_host_api_client_get_config(self):
        """Test Host API client getting hotel config."""
        with patch("httpx.AsyncClient") as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json = Mock(
                return_value={
                    "hotelCode": "HOTEL001",
                    "hotelName": "Sample Hotel",
                    "rooms": [],
                    "segments": [],
                }
            )

            mock_async_client = AsyncMock()
            mock_async_client.request = AsyncMock(return_value=mock_response)
            mock_async_client.__aenter__ = AsyncMock(return_value=mock_async_client)
            mock_async_client.__aexit__ = AsyncMock(return_value=None)

            mock_client.return_value = mock_async_client

            client = HostPMSAPIClient()
            config = await client.get_hotel_config("HOTEL001")

            assert config["hotelCode"] == "HOTEL001"


class TestOrchestrationIntegration:
    """Integration tests for the main orchestrator."""

    @pytest.mark.asyncio
    async def test_orchestrator_process_single_hotel(
        self,
        mock_esb_client,
        mock_host_api_client,
        mock_s3_manager,
        mock_sqs_manager,
    ):
        """Test orchestrator processing a single hotel."""
        orchestrator = HostPMSConnectorOrchestrator()

        # Run with mocks
        result = await orchestrator.process_hotel("HOTEL001")

        assert result["hotel_code"] == "HOTEL001"
        assert "config" in result
        assert isinstance(result["errors"], list)

    @pytest.mark.asyncio
    async def test_orchestrator_process_all_hotels(
        self,
        mock_esb_client,
        mock_host_api_client,
        mock_s3_manager,
        mock_sqs_manager,
    ):
        """Test orchestrator processing all hotels."""
        orchestrator = HostPMSConnectorOrchestrator()

        # Run with mocks
        result = await orchestrator.process_all_hotels()

        assert result["total_hotels"] >= 0
        assert "hotels" in result
        assert "start_time" in result
        assert "end_time" in result

    @pytest.mark.asyncio
    async def test_orchestrator_handles_api_errors(
        self,
        mock_esb_client,
        mock_host_api_client,
        mock_s3_manager,
        mock_sqs_manager,
    ):
        """Test orchestrator gracefully handles API errors."""
        # Make ESB client fail
        mock_esb_client.get_hotel_parameters.side_effect = Exception(
            "API Error"
        )

        orchestrator = HostPMSConnectorOrchestrator()
        result = await orchestrator.process_hotel("HOTEL001")

        assert result["success"] is False
        assert len(result["errors"]) > 0


class TestDataSerialization:
    """Tests for data serialization and format consistency."""

    def test_reservation_collection_serialization(self):
        """Test that ReservationCollection can be serialized to JSON."""
        collection = ReservationCollection(
            reservations=[],
            hotel_code="HOTEL001",
            total_count=0,
        )

        # Should not raise
        json_str = collection.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["hotelCode"] == "HOTEL001"
        assert parsed["totalCount"] == 0

    def test_pydantic_model_json_serialization(self):
        """Test that Pydantic models serialize to JSON correctly."""
        from src.models.climber.config import HotelConfigData, RoomDefinition

        config = HotelConfigData(
            hotel_code="HOTEL001",
            hotel_name="Test Hotel",
            rooms=[RoomDefinition(code="D", name="Double", capacity=2)],
            room_count=1,
        )

        json_str = config.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["hotelCode"] == "HOTEL001"
        assert len(parsed["rooms"]) == 1
        assert parsed["rooms"][0]["code"] == "D"
