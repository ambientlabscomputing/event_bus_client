# GitHub Pages Setup Instructions

This guide explains how to enable GitHub Pages for this repository to host the documentation.

## Enable GitHub Pages

1. **Go to Repository Settings**
   - Navigate to: `https://github.com/ambientlabscomputing/event_bus_client/settings`

2. **Navigate to Pages Section**
   - In the left sidebar, click on **"Pages"**

3. **Configure Source**
   - Under "Build and deployment"
   - Source: Select **"GitHub Actions"** (not the old "Deploy from a branch" method)
   - This will use the `.github/workflows/pages.yml` workflow

4. **Save and Wait**
   - The workflow will automatically run when you push to the `main` branch
   - First deployment takes 2-3 minutes
   - Subsequent updates are faster

## Verify Deployment

Once the workflow completes:

1. Check the Actions tab: `https://github.com/ambientlabscomputing/event_bus_client/actions`
2. Look for the "Deploy Documentation" workflow
3. Once it shows a green checkmark, your site is live at:
   ```
   https://ambientlabscomputing.github.io/event_bus_client/
   ```

## Manual Trigger (Optional)

If you need to manually trigger documentation deployment:

1. Go to Actions tab
2. Select "Deploy Documentation" workflow
3. Click "Run workflow" button
4. Select the `main` branch
5. Click "Run workflow"

## Workflow Details

The Pages deployment workflow (`.github/workflows/pages.yml`) automatically:

- Runs on every push to `main` branch
- Collects all documentation files
- Creates a Jekyll-based site
- Deploys to GitHub Pages
- Provides navigation between docs

## Updating Documentation

To update the documentation:

1. Make changes to any `.md` files
2. Commit and push to `main` branch
3. GitHub Actions will automatically rebuild and deploy
4. Changes appear at the GitHub Pages URL within 2-3 minutes

## Jekyll Theme

The site uses the **Cayman** theme by default. To change:

Edit the `_config.yml` section in `.github/workflows/pages.yml`:

```yaml
theme: jekyll-theme-cayman  # Change to your preferred theme
```

Available themes:
- `jekyll-theme-cayman`
- `jekyll-theme-minimal`
- `jekyll-theme-architect`
- `jekyll-theme-slate`
- `jekyll-theme-dinky`

## Custom Domain (Optional)

To use a custom domain:

1. Add a `CNAME` file to the repository root with your domain
2. Configure DNS with your domain provider
3. In GitHub Settings > Pages, add your custom domain

Example CNAME file:
```
docs.ambientlabs.io
```

## Troubleshooting

### Build Fails

Check the Actions log for errors:
1. Go to Actions tab
2. Click on the failed workflow run
3. Expand the failed step to see error details

Common issues:
- Invalid markdown syntax
- Missing files referenced in navigation
- Incorrect file paths

### Pages Not Showing

1. Verify GitHub Actions workflow completed successfully
2. Check repository settings > Pages shows the correct source
3. Wait 5-10 minutes for DNS propagation
4. Try accessing in incognito/private browsing mode

### 404 Errors

- Ensure all internal links use relative paths
- Check file names match exactly (case-sensitive)
- Verify files exist in the repository

## Site Structure

The deployed site will have this structure:

```
https://ambientlabscomputing.github.io/event_bus_client/
├── index.md              (Main README)
├── contributing.md       (Contributing guide)
├── changelog.md          (Version history)
├── loadtest.md          (Load testing docs)
├── docs/                (Test results and guides)
│   ├── README.md
│   ├── QUICK_REFERENCE.md
│   └── (test result files)
└── loadtest-examples/   (Example configs)
    ├── quick.json
    ├── basic.json
    ├── stress.json
    └── accuracy.json
```

## Access Control

GitHub Pages for public repositories are:
- ✅ Publicly accessible (anyone can view)
- ✅ Free hosting
- ✅ Automatic HTTPS
- ✅ Custom domains supported

For private repositories:
- Requires GitHub Pro, Team, or Enterprise
- Can restrict access to organization members

## Next Steps

After enabling GitHub Pages:

1. Update README.md badge with actual GitHub Pages URL
2. Add documentation link to repository description
3. Share the documentation URL with your team
4. Consider adding a "View Documentation" button to README

---

**Once GitHub Pages is enabled, documentation will be available at:**
**https://ambientlabscomputing.github.io/event_bus_client/**
