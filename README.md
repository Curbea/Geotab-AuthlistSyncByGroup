WIP,

A problem with Geotab is that by default any nfc tag from them can be used in the vehicle; if that tag is not registered in the database, 
the vehicle is assigned to the "Unknown Driver" for the duration of the trip. The problem with this is that "Unknown Driver" does not appear in any reporting done on user's, from a liability reduction standpoint this is an issue;
Mainly this will also add precious time to any accident response.

Geotab has a fairly unmentioned feature for it's nfc device's: A whitelist of nfc tags can be provided to the device, and stored locally on the device, 
to ensure that for every driver assignment event we end up with an actual driver

This script is fundamentally designed to synchronize a group of drivers with a group of vehicles

For every vehicle missing the required parameter it will be added to the device, but set to disabled. This is to ensure that the device recieves it's whitelist. This will need to be manually enabled in the advanced section of the asset.

For vehicles that are no longer assigned to their original group, they will recieve a clear whitelist message, before recieving a new whitelist.

This is intended to be run through cron

To do:
Add a polling mechanism prior to sending off 
Add some routine user modifications for new hires
Add a post run that verifies receipt of the text messages
Set the parameter to disabled before the clear message is sent
