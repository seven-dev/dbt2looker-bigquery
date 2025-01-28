import warnings


class DeprecationWarning(UserWarning):
    pass


captured_warnings = []


def capture_warning(message, category, filename, lineno, file=None, line=None):
    if issubclass(category, DeprecationWarning):
        captured_warnings.append((message, category, filename, lineno))


warnings.showwarning = capture_warning

warnings.simplefilter("always", DeprecationWarning)  # Capture our custom warnings
