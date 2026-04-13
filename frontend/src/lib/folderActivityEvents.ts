const FOLDER_ACTIVITY_UPDATED_EVENT = 'classifier:folder-activity-updated'

export function notifyFolderActivityUpdated() {
  if (typeof window === 'undefined') return
  window.dispatchEvent(new CustomEvent(FOLDER_ACTIVITY_UPDATED_EVENT))
}

export function subscribeFolderActivityUpdated(listener: () => void) {
  if (typeof window === 'undefined') {
    return () => {}
  }

  const handler = () => {
    listener()
  }

  window.addEventListener(FOLDER_ACTIVITY_UPDATED_EVENT, handler)
  return () => {
    window.removeEventListener(FOLDER_ACTIVITY_UPDATED_EVENT, handler)
  }
}
