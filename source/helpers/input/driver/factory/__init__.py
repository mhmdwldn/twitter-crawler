"""Input driver factory."""

from helpers.input.driver.std import StdInputDriver


class InputDriverFactory:
    """Factory that creates the correct InputDriver."""

    @staticmethod
    def create_input_driver(*args, **kwargs):
        """Create an input driver — currently only STD; extensible for queues."""
        return InputDriverFactory.create_std_input_driver(*args, **kwargs)

    @staticmethod
    def create_std_input_driver(*args, **kwargs):
        """Create a StdInputDriver reading jobs from ``input`` (JSON file path)."""
        return StdInputDriver(kwargs.pop("input", None), *args, **kwargs)
