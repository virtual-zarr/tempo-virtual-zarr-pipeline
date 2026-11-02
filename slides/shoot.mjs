// Capture the titiler-multidim viewer with and without the where filter,
// at the same extent, for the "Same filter, two ways" slide.
//   npx playwright install chromium   (once)
//   node shoot.mjs
import { chromium } from 'playwright'

const API = 'https://eic-staging.staging.earth.gov/api/titiler-multidim'
const common = [
  ['url', 's3://airquality-data-store-develop/tempo/no2/v04/'],
  ['variable', 'vertical_column_troposphere'],
  ['sel', 'time=nearest::2026-09-18T21:14:15Z'],
  ['algorithm', 'mean'],
  ['tilesize', '512'],
  ['resampling', 'nearest'],
  ['colormap_name', 'magma_r'],
  ['rescale', '0,30000000000000000'],
]
const where = [
  ['where', 'eff_cloud_fraction<=0.5'],
  ['where', 'solar_zenith_angle<=80'],
  ['where', 'main_data_quality_flag<=1'],
]
// Tile 5/4/11 in WebMercatorQuad: lon -135 to -123.75, lat 40.98 to 48.92
const bounds = [[40.98, -135], [48.92, -123.75]]

const shots = [
  ['public/screenshots/titiler-nofilter.png', common],
  ['public/screenshots/titiler-filter.png', [...common, ...where]],
]

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } })
for (const [file, params] of shots) {
  await page.goto(`${API}/WebMercatorQuad/map?${new URLSearchParams(params)}`)
  await page.waitForFunction(() => window.map && window.map._loaded)
  await page.evaluate((b) => {
    map.eachLayer((l) => l instanceof L.GeoJSON && map.removeLayer(l)) // drop the cyan extent box
    map.fitBounds(b)
  }, bounds)
  await page.waitForLoadState('networkidle')
  await page.waitForTimeout(3000)
  await page.screenshot({ path: file })
  console.log('wrote', file)
}
await browser.close()
