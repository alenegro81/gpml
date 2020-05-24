make

TODO: 
 - make directory names configurable - for example, grab them from command line
 - also make password configurable
 - libraries dependencies need to be set to specific versions
 - make encryption configurable. otherwise specify `encrypted=0`
 - should we clear all data & constraints?
 - fix the parallel version as well
 - fix errors like: `(KeyError('writers',), ['99', '0113283', '63076'], 89L)`
 - user ratings aren't loaded...

Exceptions are not handled correctly when they are already exist

```
neobolt.exceptions.ClientError: An equivalent constraint already exists, 'Constraint( UNIQUE, :Movie(movieId) )'.
```

