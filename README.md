WIP

This functions to set the Authorized Drivers List and sync it to all drivers in the same group as the vehicle

This is intended to be thrown onto Cron or taskscheduler

Other methods of managing the auth list such as the addins will work in conjunction with this, I would recommend them to enable the auth list on the iox reader, this does not do that.  
This script will not interact with edits made from the addins. 

If exceptions need to be made. I do not know yet if the Iox reader would store duplicates of the keys in the case of overlapping groups.

If you send a clear auth list command from the ui, ensure that you delete that vehicle entry from the table else only new changes will be commited.
