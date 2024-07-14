# Geotab NFC Key Management Script

## Overview

This script is designed to manage and synchronize NFC keys for drivers across various vehicles in a fleet. It uses the MyGeotab API to fetch, update, and manage driver and vehicle information. The primary goal is to ensure that each vehicle has a whitelist of authorized NFC tags, reducing liability issues and improving the accuracy of driver assignments.

## Features

- **NFC Key Management**: Ensures that only registered NFC tags are used in vehicles, preventing the "Unknown Driver" issue.
- **Database Storage**: Stores keys and user information in an SQLite database for persistent storage and comparison.
- **Vehicle and Driver Synchronization**: Synchronizes drivers with vehicles, updating their authorization lists as needed.
- **Retry Mechanism**: Implements a retry mechanism for failed key updates, ensuring robustness.
- **Logging**: Logs all significant events and errors to a log file for monitoring and troubleshooting.

## Requirements

- Python 3.x
- MyGeotab API credentials
- SQLite3
- dotenv

## Setup

1. **Install Dependencies**:
   ```python
   pip install mygeotab python-dotenv

2. **Edit .env**

|  ENV Parameters                       |
|---------------------------------------|
| GEOTAB_USERNAME=your_username         |
| GEOTAB_PASSWORD=your_password         |
| GEOTAB_DATABASE=your_database         |
| GEOTAB_GROUPS=group1,group2,group3    |
| PATCH_USERS=False                     |
| PATCH_ASSETS=False                    |
| PATCH_TZ=False                        |
| PATCH_SC=False                        |
| NEW_SC_ID=new_security_group_id       |
| OLD_SC_ID=old_security_group_id, old_security_group_id2       |
| EXCEPTION_GROUP_ID=exception_group_id |

Geotab_Groups is the name of each group

Id's can be found easily from geotab; you can find them by looking at the url when viewing the groups, it is a block three or four digits at the end of the url.

For each Geotab_Groups listed, if you are looking to set timezones, create a line in the .env file with the group's name = and the timezone to update to e.g.

Group Vancouver=America/Vancouver
Check geotab to see availible timezone options

3. **Launch**:
   ```python
   python3 main.py

4. **Schedule**
   ```bash
   crontab -e
