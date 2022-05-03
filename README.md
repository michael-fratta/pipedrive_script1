A Python script - running automatically, on a (hardcoded) scheduler; bundled as an app and hosted on the cloud platform Heroku - that, essentially, updates the relevant entities within a CRM (Pipedrive) with the contents of an XLSX file fetched from an SFTP server. The steps it follows are explained - concisely - below (see code for full detail):

• connects to an SFTP server, using the pysftp library, and attempts to get the latest file that matches the provided search string

• if a file is found, it iterates through each row within the file - appending the relevant unique identifiers (in this case - Pipedrive Deal entity IDs) to a list

• it iterates through each of these IDs - querying Pipedrive using the Pipedrive API, to get that entity's data

• it then checks if there are any differences between the corresponding values for that Deal ID - within the XLSX file - and the Pipedrive Deal entities

• extensive mappings (assigning Pipedrive keys or values to a human-readable variable, or working out their Pipedrive equivalents based on given business rules) are required, to shape the data in such a way that Pipedrive understands, and to achieve successful PUT requests - as the data (contained within the XLSX file) comes from a CRM that behaves, and stores data, differently to Pipedrive: called Key2

• if there are any differences - then only those different values are updated in Pipedrive, with the data from the XLSX file

• posts relevant updates/actions to a dedicated Slack (messaging service) channel, as a message, via the Slack API.

I am the sole author of this script. Revealing keys/values/variables/file names have been replaced with arbitrary/generic ones - for demonstrative purposes only.
