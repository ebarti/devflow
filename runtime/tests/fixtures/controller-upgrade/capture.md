The compressed module contains the exact native preparation source from PR73
at `e50e66f3eb416d7361c3e553d761e22e8a8e2f06`, captured with `git show`.
Its decompressed SHA-256 is
`2ce61db4129f91925d841bd38c2a3ffdb5a18b25ae8123865bdd92b06f1c36bf`.
The tests use it for real previous-source preparation in a disposable installed
runtime, then replace it with candidate bytes at the same owned path. Successful
upgrade cases retain original proof/specification bytes and replay unedited history.
