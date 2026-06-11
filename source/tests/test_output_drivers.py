"""Tests for helpers/output/driver/* — output drivers."""

from __future__ import annotations

import os
import tempfile

import pytest

from exception.exception import OutputDriverNotRecognizeException
from helpers.output.driver.factory import OutputDriverFactory
from helpers.output.driver.file import FileOutputDriver
from helpers.output.driver.std import StdOutputDriver


class TestOutputDriverFactory:
    def test_create_std_driver(self) -> None:
        driver = OutputDriverFactory.create_output_driver(destination="std")
        assert isinstance(driver, StdOutputDriver)
        assert driver.name == "std"

    @pytest.mark.asyncio
    async def test_create_file_driver(self) -> None:
        path = tempfile.mktemp(suffix=".json")
        driver = OutputDriverFactory.create_output_driver(
            destination="file", output=path,
        )
        try:
            assert isinstance(driver, FileOutputDriver)
            assert driver.name == "file"
        finally:
            await driver.close()
            if os.path.exists(path):
                os.remove(path)

    def test_missing_destination_raises(self) -> None:
        with pytest.raises(OutputDriverNotRecognizeException):
            OutputDriverFactory.create_output_driver()

    def test_unknown_destination_raises(self) -> None:
        with pytest.raises(OutputDriverNotRecognizeException):
            OutputDriverFactory.create_output_driver(destination="unknown")


class TestStdOutputDriver:
    @pytest.mark.asyncio
    async def test_put_prints(self, capsys) -> None:
        driver = StdOutputDriver()
        await driver.put("hello world")
        captured = capsys.readouterr()
        assert "hello world" in captured.out

    @pytest.mark.asyncio
    async def test_close_noop(self) -> None:
        driver = StdOutputDriver()
        await driver.close()  # should not raise


class TestFileOutputDriver:
    @pytest.mark.asyncio
    async def test_put_writes_to_file(self) -> None:
        path = tempfile.mktemp(suffix=".json")
        driver = FileOutputDriver(path=path)
        try:
            await driver.put("line1")
            await driver.put("line2")
            driver.file.flush()  # ensure buffered writes hit disk

            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            assert len(lines) == 2
        finally:
            await driver.close()
            if os.path.exists(path):
                os.remove(path)

    @pytest.mark.asyncio
    async def test_put_without_path_raises(self) -> None:
        driver = FileOutputDriver()
        with pytest.raises(RuntimeError):
            await driver.put("orphan line")

    @pytest.mark.asyncio
    async def test_close(self) -> None:
        path = tempfile.mktemp(suffix=".json")
        driver = FileOutputDriver(path=path)
        await driver.close()
        assert driver.file is None
        if os.path.exists(path):
            os.remove(path)
