import logging
import re

log = logging.getLogger(__name__)

# These appear exactly as they are listed for each result. These are always
# checked first before other regexes.
COMMON_IMAGE_NAMES = [
    'scattering_pattern_log.png',
    'scattering_pattern_pre.png',
    'scattering_pattern_pre_log.png',
]

# These are prefixed with the HDF Sample name.
# EX:A079_AMJ290_P2VP40_S03_Ann190C_att4_175C_Lq0_001_0001-0300_intensity_t.png
COMMON_REGEXES = [
    r'.+_corr_params.png',
]

# These will match any range for a given typed group of images.
# EX: <SAMPLE>_g2_corr_fit000_008.png
RANGE_REGEXES = [
    r'.+(_g2_corr_fit)(\d\d\d)_(\d\d\d).png',
    r'.+(_g2_corr_)(\d\d\d)_(\d\d\d).png',
]

SHOW_BY_DEFAULT = [
    'scattering_pattern_log.png',
    'total_intensity_vs_time.png',
    r'.+_g2_corr_fit000_008.png',
    r'.+_intensity.png',
    r'.+_intensity_t.png',
]

# A template for constructing a regex for the RANGE_REGEXES above.
RANGE_REGEX_TEMPLATE = r'.+({name})({low})_({high}).png'


def regex_for_filename(filename):
    return (
        filename if filename in COMMON_IMAGE_NAMES else None or
        check_common(filename) or
        check_range(filename) or
        # Simply return the filename if there are no other entries for it
        # This is a fallback if a new image was added that we don't track
        filename
    )


def check_common(filename):
    for common_regex in COMMON_REGEXES:
        if re.match(common_regex, filename):
            return common_regex


def check_range(filename):
    for range_regex in RANGE_REGEXES:
        match = re.match(range_regex, filename)
        if match:
            fmt_strings = zip(['name', 'low', 'high'], match.groups())
            return RANGE_REGEX_TEMPLATE.format(**dict(fmt_strings))
