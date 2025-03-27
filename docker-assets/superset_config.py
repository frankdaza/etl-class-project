import os

# Read SECRET_KEY from the environment or fall back to a dummy if missing
SECRET_KEY = os.getenv("SECRET_KEY", "dummy-override-if-missing")
