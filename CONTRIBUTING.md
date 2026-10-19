# Contributing to kubed

Thanks for helping. Bug reports, ideas, docs fixes and code are all welcome.

## Reporting a bug

Open an issue with:
- what you did (the keys you pressed),
- what kubed showed, and what `kubectl` says is true,
- `kubed --version`, your OS, and your Kubernetes version.

A screenshot helps. Please hide anything private (cluster, app or secret names).

## Changing code

1. Fork the repository and make a branch.
2. Make your change. Keep it focused: one fix or feature per pull request.
3. Add or update a test. A bug fix needs a test that fails without the fix.
4. Run the checks, all of which must pass:

   ```bash
   make lint
   go test ./...
   ```

5. Open a pull request and explain what changed and why.

Every pull request needs a review from the maintainer and passing CI before
it can be merged. Pull requests are squash-merged.

## Words on screen

kubed talks to people, not to Kubernetes experts. Text on screen should be
plain words a new user understands: say what is wrong and what to do next.
No jargon where a plain word works.

## Code of conduct

Be kind and assume good intent. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
