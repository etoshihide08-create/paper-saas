function toggleOrganizePanel(panelId) {
  const panel = document.getElementById(panelId);
  if (!panel) return;
  panel.classList.toggle("open");
}

function setInlineMessage(id, text, type) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text;
  el.className = "inline-message " + type;
}

async function submitMove(event, pubmedId, currentFolderName) {
  event.preventDefault();
  const form = event.target;
  const formData = new FormData(form);

  try {
    const response = await fetch(`/saved/${pubmedId}/move`, {
      method: "POST",
      body: formData,
    });
    const data = await response.json();

    if (response.ok && data.ok) {
      setInlineMessage(
        `move-message-${pubmedId}`,
        "フォルダを変更しました。移動先で確認できます。",
        "success",
      );

      if (data.folder_name !== currentFolderName) {
        const card = document.getElementById(`paper-card-${pubmedId}`);
        if (card) {
          card.style.opacity = "0.4";
          setTimeout(() => {
            card.remove();
          }, 180);
        }
      }
    } else {
      setInlineMessage(
        `move-message-${pubmedId}`,
        data.message || "フォルダ変更に失敗しました",
        "error",
      );
    }
  } catch (error) {
    setInlineMessage(
      `move-message-${pubmedId}`,
      "フォルダ変更に失敗しました",
      "error",
    );
  }
}

async function submitRename(event, pubmedId) {
  event.preventDefault();
  const form = event.target;
  const formData = new FormData(form);

  try {
    const response = await fetch(`/saved/${pubmedId}/rename`, {
      method: "POST",
      body: formData,
    });
    const data = await response.json();

    if (response.ok && data.ok) {
      const titleAnchor = document.querySelector(`#paper-title-${pubmedId} a`);
      if (titleAnchor) {
        titleAnchor.textContent = data.display_title;
      }
      setInlineMessage(
        `rename-message-${pubmedId}`,
        "表示名を更新しました",
        "success",
      );
    } else {
      setInlineMessage(
        `rename-message-${pubmedId}`,
        data.message || "表示名の更新に失敗しました",
        "error",
      );
    }
  } catch (error) {
    setInlineMessage(
      `rename-message-${pubmedId}`,
      "表示名の更新に失敗しました",
      "error",
    );
  }
}

async function submitNote(event, pubmedId) {
  event.preventDefault();
  const form = event.target;
  const formData = new FormData(form);

  try {
    const response = await fetch(`/saved/${pubmedId}/note`, {
      method: "POST",
      body: formData,
    });
    const data = await response.json();

    if (response.ok && data.ok) {
      const noteBox = document.getElementById(`paper-note-${pubmedId}`);
      if (noteBox) {
        if (data.user_note) {
          noteBox.textContent = data.user_note;
          noteBox.style.display = "block";
        } else {
          noteBox.textContent = "";
          noteBox.style.display = "none";
        }
      }
      setInlineMessage(
        `note-message-${pubmedId}`,
        "メモを保存しました",
        "success",
      );
    } else {
      setInlineMessage(
        `note-message-${pubmedId}`,
        data.message || "メモ保存に失敗しました",
        "error",
      );
    }
  } catch (error) {
    setInlineMessage(
      `note-message-${pubmedId}`,
      "メモ保存に失敗しました",
      "error",
    );
  }
}
