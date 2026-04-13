import { useEffect, useMemo, useState } from 'react'
import { FolderSearch, Plus, Trash2 } from 'lucide-react'

import { getConfig, updateConfig } from '@/api/config'
import { DirPicker } from '@/components/DirPicker'
import { useConfigStore } from '@/store/configStore'
import type { AppConfig } from '@/types'

type OutputDirKey = keyof NonNullable<AppConfig['output_dirs']>

interface FormState {
  scanInputDirs: string[]
  outputDirs: NonNullable<AppConfig['output_dirs']>
}

type PickerTarget =
  | { kind: 'scan'; index: number }
  | { kind: 'output'; key: OutputDirKey; index: number }

const OUTPUT_DIR_KEYS: OutputDirKey[] = ['video', 'manga', 'photo', 'other', 'mixed']

const INITIAL_FORM: FormState = {
  scanInputDirs: [''],
  outputDirs: {
    video: [''],
    manga: [''],
    photo: [''],
    other: [''],
    mixed: [''],
  },
}

const OUTPUT_DIR_LABELS: Record<OutputDirKey, string> = {
  video: '视频输出目录',
  manga: '漫画输出目录',
  photo: '写真输出目录',
  other: '其他输出目录',
  mixed: '混合输出目录',
}

function ensureOutputDirList(values: string[] | undefined): string[] {
  if (!values || values.length === 0) {
    return ['']
  }
  return values
}

export default function SettingsPage() {
  const [form, setForm] = useState<FormState>(INITIAL_FORM)
  const [isLoading, setIsLoading] = useState(true)
  const [isSaving, setIsSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)
  const [pickerOpen, setPickerOpen] = useState(false)
  const [pickerTarget, setPickerTarget] = useState<PickerTarget>({ kind: 'scan', index: 0 })
  const { scanInputDirs, load: loadConfigStore } = useConfigStore()

  const pickerInitialPath = useMemo(() => {
    const first = scanInputDirs.find((item) => item.trim() !== '')
    return first ?? '/'
  }, [scanInputDirs])

  useEffect(() => {
    let active = true

    async function loadConfig() {
      try {
        const response = await getConfig()
        if (!active) return

        setForm({
          scanInputDirs: response.data.scan_input_dirs?.length ? response.data.scan_input_dirs : [''],
          outputDirs: {
            video: ensureOutputDirList(response.data.output_dirs?.video),
            manga: ensureOutputDirList(response.data.output_dirs?.manga),
            photo: ensureOutputDirList(response.data.output_dirs?.photo),
            other: ensureOutputDirList(response.data.output_dirs?.other),
            mixed: ensureOutputDirList(response.data.output_dirs?.mixed),
          },
        })
        setError(null)
      } catch (loadError) {
        if (!active) return
        setError(loadError instanceof Error ? loadError.message : '加载配置失败')
      } finally {
        if (active) setIsLoading(false)
      }
    }

    void loadConfig()
    return () => {
      active = false
    }
  }, [])

  useEffect(() => {
    void loadConfigStore()
  }, [loadConfigStore])

  function setPickerValue(path: string) {
    setForm((prev) => {
      if (pickerTarget.kind === 'scan') {
        const nextScanDirs = [...prev.scanInputDirs]
        nextScanDirs[pickerTarget.index] = path
        return { ...prev, scanInputDirs: nextScanDirs }
      }

      return {
        ...prev,
        outputDirs: {
          ...prev.outputDirs,
          [pickerTarget.key]: prev.outputDirs[pickerTarget.key].map((item, index) => (
            index === pickerTarget.index ? path : item
          )),
        },
      }
    })
    setPickerOpen(false)
  }

  function addScanInputDirRow() {
    setForm((prev) => ({ ...prev, scanInputDirs: [...prev.scanInputDirs, ''] }))
  }

  function removeScanInputDirRow(index: number) {
    setForm((prev) => {
      const next = prev.scanInputDirs.filter((_, i) => i !== index)
      return { ...prev, scanInputDirs: next.length > 0 ? next : [''] }
    })
  }

  function updateScanInputDirRow(index: number, value: string) {
    setForm((prev) => ({
      ...prev,
      scanInputDirs: prev.scanInputDirs.map((item, i) => (i === index ? value : item)),
    }))
  }

  function addOutputDirRow(key: OutputDirKey) {
    setForm((prev) => ({
      ...prev,
      outputDirs: {
        ...prev.outputDirs,
        [key]: [...prev.outputDirs[key], ''],
      },
    }))
  }

  function removeOutputDirRow(key: OutputDirKey, index: number) {
    setForm((prev) => {
      const nextRows = prev.outputDirs[key].filter((_, i) => i !== index)
      return {
        ...prev,
        outputDirs: {
          ...prev.outputDirs,
          [key]: nextRows.length > 0 ? nextRows : [''],
        },
      }
    })
  }

  function updateOutputDirRow(key: OutputDirKey, index: number, value: string) {
    setForm((prev) => ({
      ...prev,
      outputDirs: {
        ...prev.outputDirs,
        [key]: prev.outputDirs[key].map((item, i) => (i === index ? value : item)),
      },
    }))
  }

  async function handleSubmit(e: { preventDefault(): void }) {
    e.preventDefault()
    setIsSaving(true)
    setError(null)
    setSuccess(null)

    try {
      const cleanedScanInputDirs = form.scanInputDirs.map((item) => item.trim()).filter((item) => item.length > 0)
      const cleanedOutputDirs = OUTPUT_DIR_KEYS.reduce((acc, key) => {
        acc[key] = form.outputDirs[key].map((item) => item.trim()).filter((item) => item.length > 0)
        return acc
      }, {} as NonNullable<AppConfig['output_dirs']>)
      await updateConfig({
        scan_input_dirs: cleanedScanInputDirs,
        output_dirs: cleanedOutputDirs,
      })
      await loadConfigStore(true)
      setSuccess('配置已保存')
    } catch (saveError) {
      setError(saveError instanceof Error ? saveError.message : '保存失败')
    } finally {
      setIsSaving(false)
    }
  }

  return (
    <section className="mx-auto max-w-3xl px-6 py-8">
      <h1 className="mb-8 text-3xl font-black tracking-tight uppercase">系统配置</h1>

      <form onSubmit={(e) => void handleSubmit(e)} className="space-y-8">
        <div className="space-y-4 border-2 border-foreground bg-card p-6 shadow-hard">
          <div className="flex items-center justify-between gap-4">
            <div>
              <label className="block text-sm font-black tracking-widest">扫描输入目录（可多个）</label>
              <p className="mt-1 text-xs font-bold text-muted-foreground">手动扫描和扫描计划任务围绕这组目录工作。</p>
            </div>
            <button
              type="button"
              onClick={addScanInputDirRow}
              className="flex items-center gap-2 border-2 border-foreground bg-background px-4 py-2 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
            >
              <Plus className="h-4 w-4" />
              添加目录
            </button>
          </div>
          <div className="space-y-2">
            {form.scanInputDirs.map((scanInputDir, index) => (
              <div key={index} className="flex gap-2">
                <input
                  value={scanInputDir}
                  onChange={(event) => updateScanInputDirRow(index, event.target.value)}
                  className="w-full border-2 border-foreground bg-background px-4 py-3 text-sm font-mono font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
                  placeholder="/data/source"
                />
                <button
                  type="button"
                  onClick={() => {
                    setPickerTarget({ kind: 'scan', index })
                    setPickerOpen(true)
                  }}
                  className="border-2 border-foreground bg-background px-3 py-2 text-foreground transition-all hover:bg-foreground hover:text-background"
                >
                  <FolderSearch className="h-4 w-4" />
                </button>
                <button
                  type="button"
                  onClick={() => removeScanInputDirRow(index)}
                  className="border-2 border-red-900 bg-red-100 px-3 py-2 text-red-900 transition-all hover:bg-red-900 hover:text-red-100"
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              </div>
            ))}
          </div>
        </div>

        <div className="space-y-4">
          <div>
            <label className="block text-sm font-black tracking-widest">分类输出目录</label>
            <p className="mt-1 text-xs font-bold text-muted-foreground">同一分类可配置多个目录，第一项为默认目录，工作流节点可选择其中某一个具体目录。</p>
          </div>
          <div className="space-y-4">
            {OUTPUT_DIR_KEYS.map((key) => (
              <div key={key} className="border-2 border-foreground bg-card p-5 shadow-hard transition-all hover:-translate-y-1 hover:shadow-hard-hover">
                <div className="mb-3 flex items-start justify-between gap-3">
                  <div>
                    <p className="text-sm font-black">{OUTPUT_DIR_LABELS[key]}</p>
                    <p className="mt-1 text-[10px] font-bold text-muted-foreground">对应 `{key}` 分类，第 1 项默认</p>
                  </div>
                  <button
                    type="button"
                    onClick={() => addOutputDirRow(key)}
                    className="flex items-center gap-2 border-2 border-foreground bg-background px-3 py-1.5 text-xs font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
                  >
                    <Plus className="h-4 w-4" />
                    添加目录
                  </button>
                </div>
                <div className="space-y-2">
                  {form.outputDirs[key].map((value, index) => (
                    <div key={`${key}-${index}`} className="flex gap-2">
                      <input
                        value={value}
                        onChange={(event) => updateOutputDirRow(key, index, event.target.value)}
                        className="w-full border-2 border-foreground bg-background px-3 py-2 text-xs font-mono font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-1"
                        placeholder={`/data/target/${key}`}
                      />
                      <button
                        type="button"
                        onClick={() => {
                          setPickerTarget({ kind: 'output', key, index })
                          setPickerOpen(true)
                        }}
                        className="border-2 border-foreground bg-background px-3 py-2 text-foreground transition-all hover:bg-foreground hover:text-background"
                      >
                        <FolderSearch className="h-4 w-4" />
                      </button>
                      <button
                        type="button"
                        onClick={() => removeOutputDirRow(key, index)}
                        className="border-2 border-red-900 bg-red-100 px-3 py-2 text-red-900 transition-all hover:bg-red-900 hover:text-red-100"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="border-2 border-dashed border-foreground bg-muted/10 px-4 py-4 text-xs font-bold text-muted-foreground">
          部署级根目录（如 SOURCE_DIR / TARGET_DIR）为只读信息，需通过 Docker 挂载与环境变量修改，应用运行中不可改。
        </div>

        {error && <p className="border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900 shadow-hard">{error}</p>}
        {success && <p className="border-2 border-green-900 bg-green-100 px-4 py-3 text-sm font-bold text-green-900 shadow-hard">{success}</p>}

        <div className="flex justify-end pt-4">
          <button
            type="submit"
            disabled={isLoading || isSaving}
            className="border-2 border-foreground bg-primary px-8 py-3 text-sm font-black tracking-widest text-primary-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-primary disabled:hover:text-primary-foreground disabled:hover:shadow-none disabled:hover:translate-y-0"
          >
            {isSaving ? '保存中…' : '保存配置'}
          </button>
        </div>
      </form>

      <DirPicker
        open={pickerOpen}
        initialPath={pickerInitialPath}
        title={pickerTarget.kind === 'scan' ? '选择扫描输入目录' : '选择输出目录'}
        onConfirm={setPickerValue}
        onCancel={() => setPickerOpen(false)}
      />
    </section>
  )
}
