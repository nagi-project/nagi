from pytest_mock import MockerFixture

from nagi_cli.output import echo_output


class TestEchoOutput:
    def test_uses_pager_when_tty(self, mocker: MockerFixture) -> None:
        mocker.patch("nagi_cli.output.sys.stdout.isatty", return_value=True)
        mock_pager = mocker.patch("nagi_cli.output.click.echo_via_pager")
        echo_output("hello", no_pager=False)
        mock_pager.assert_called_once_with("hello")

    def test_no_pager_when_not_tty(self, mocker: MockerFixture) -> None:
        mocker.patch("nagi_cli.output.sys.stdout.isatty", return_value=False)
        mock_echo = mocker.patch("nagi_cli.output.click.echo")
        echo_output("hello", no_pager=False)
        mock_echo.assert_called_once_with("hello")

    def test_no_pager_flag_disables_pager(self, mocker: MockerFixture) -> None:
        mocker.patch("nagi_cli.output.sys.stdout.isatty", return_value=True)
        mock_echo = mocker.patch("nagi_cli.output.click.echo")
        echo_output("hello", no_pager=True)
        mock_echo.assert_called_once_with("hello")
