import warnings


class DeprecationWarning(UserWarning):
    pass


class CatalogWarning(UserWarning):
    pass


class ParsingWarning(UserWarning):
    pass


captured_warnings = []


def capture_warning(message, category, filename, lineno, file=None, line=None):
    captured_warnings.append((message, category, filename, lineno))


warnings.showwarning = capture_warning
