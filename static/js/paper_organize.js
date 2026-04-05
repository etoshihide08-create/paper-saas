async function submitPaperRename(event, pubmedId) {
  event.preventDefault();

  const form = event.target;
  const formData = new FormData(form);
  const message = document.getElementById("organize-message");

  try {
    const response = await fetch(`/saved/${pubmedId}/rename`, {
      method: "POST",
      body: formData,
    });

    const data = await response.json();

    if (response.ok && data.ok) {
      const titleEl = document.querySelector(".paper-title");
      if (titleEl) {
        titleEl.textContent = data.display_title;
      }

      message.textContent = "表示名を更新しました";
      message.classList.remove("error");
      message.classList.add("success");
      message.style.display = "block";
    } else {
      message.textContent = data.message || "表示名の更新に失敗しました";
      message.classList.remove("success");
      message.classList.add("error");
      message.style.display = "block";
    }
  } catch (error) {
    message.textContent = "表示名の更新に失敗しました";
    message.classList.remove("success");
    message.classList.add("error");
    message.style.display = "block";
  }
}

async function submitPaperMove(event, pubmedId) {
  event.preventDefault();

  const form = event.target;
  const formData = new FormData(form);
  const message = document.getElementById("organize-message");

  try {
    const response = await fetch(`/saved/${pubmedId}/move`, {
      method: "POST",
      body: formData,
    });

    const data = await response.json();

    if (response.ok && data.ok) {
      message.textContent = "フォルダを変更しました";
      message.classList.remove("error");
      message.classList.add("success");
      message.style.display = "block";
    } else {
      message.textContent = data.message || "フォルダ変更に失敗しました";
      message.classList.remove("success");
      message.classList.add("error");
      message.style.display = "block";
    }
  } catch (error) {
    message.textContent = "フォルダ変更に失敗しました";
    message.classList.remove("success");
    message.classList.add("error");
    message.style.display = "block";
  }
}

async function submitPaperNote(event, pubmedId) {
  event.preventDefault();

  const form = event.target;
  const formData = new FormData(form);
  const message = document.getElementById("organize-message");

  try {
    const response = await fetch(`/saved/${pubmedId}/note`, {
      method: "POST",
      body: formData,
    });

    const data = await response.json();

    if (response.ok && data.ok) {
      message.textContent = "メモを保存しました";
      message.classList.remove("error");
      message.classList.add("success");
      message.style.display = "block";
    } else {
      message.textContent = data.message || "メモ保存に失敗しました";
      message.classList.remove("success");
      message.classList.add("error");
      message.style.display = "block";
    }
  } catch (error) {
    message.textContent = "メモ保存に失敗しました";
    message.classList.remove("success");
    message.classList.add("error");
    message.style.display = "block";
  }
}
