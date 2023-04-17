# Third Party
import pytest

# Local
from drain_service.drain3.simple_profiler import SimpleProfiler


@pytest.fixture
def profiler():
    return SimpleProfiler(enclosing_section_name="total", printer=print, report_sec=30)


def test_start_section(profiler):
    profiler.start_section("sectionA")
    assert "sectionA" in profiler.section_to_stats.keys()
    assert profiler.section_to_stats["sectionA"].start_time_sec != 0
    with pytest.raises(ValueError):
        profiler.start_section("sectionA")


def test_end_section(profiler):
    profiler.start_section("sectionB")
    profiler.end_section("sectionB")
    assert "sectionB" in profiler.section_to_stats.keys()
    assert profiler.section_to_stats["sectionB"].start_time_sec == 0
    with pytest.raises(ValueError):
        profiler.end_section("sectionC")


def test_report(profiler):
    profiler.start_section("sectionD")
    profiler.end_section("sectionD")
    profiler.report()
    assert profiler.last_report_timestamp_sec != 0
