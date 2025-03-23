# "bit in that" x "Moreira's Magic"

# How to get authentication tokens

Some of the APIs we use require authentication via being logged into an account.

Everything is set up such that none of these will be committed to any repositories, instead at the base directory of the repository, place a file called `.env`, with the following:

```
AF_SESSION_ID=""
SC_ACCESS_TOKEN=""
```

You put the values of your authentication inside the quotation marks to the right of the equals sign. Instructions on obtaining these are below.

Note that the `.env` file is `.gitignore`'d so only you can see it.

## AFL Fantasy

Either use the [chrome extensions](https://github.com/bit-in-that/get_af_session) or do the following:

1. Login to AFL Fantasy using the Google Chrome
2. Open Chrome Devtools (shortcut: F12)
3. Click on the 'Application' tab
4. Click/expand 'Cookies'
5. Under Cookies, click on 'https://fantasy.afl.com.au'
6. Click on 'session' and the value should appear in the box below (you can copy and paste this)

## Supercoach

Follow the steps below:

1. Login to Supercoach  the Google Chrome
2. Open Chrome Devtools (shortcut: F12)
3. Click on the 'Application' tab
4. Click/expand 'Local storage'
5. Under Cookies, click on 'https://www.supercoach.com.au'
6. Click on 'VMLS:news:accessToken' and the value should appear in the box below (you can copy and paste this)
