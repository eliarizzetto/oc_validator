// Enable Bootstrap popovers
document.addEventListener('DOMContentLoaded', function() {
    var popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'))
    popoverTriggerList.map(function(el) {
        return new bootstrap.Popover(el)
    })
});


let currentHighlightedIssueId = null;

function highlightInvolvedElements(clickedIssue) {
    const clickedIssueId = clickedIssue.id;
    const issueColor = clickedIssue.style.backgroundColor;

    // If clicking the same issue again, clear highlights
    if (currentHighlightedIssueId === clickedIssueId) {
        clearHighlights();
        currentHighlightedIssueId = null;
        return;
    }

    // Clear any existing highlights
    clearHighlights();

    // Find all issue elements with the same id within the table
    const table = document.getElementById('table-data');
    const matchingIssues = table.querySelectorAll(`.issue-icon#${CSS.escape(clickedIssueId)}`);

    // Highlight the item-data siblings of all matching issues
    matchingIssues.forEach(issue => {
        const itemDataSibling = issue.parentElement.querySelector('.item-data');
        if (itemDataSibling) {
            itemDataSibling.style.backgroundColor = issueColor;
            itemDataSibling.classList.add('highlighted');

            // If the item-data is empty, add a visual indicator
            if (itemDataSibling.textContent.trim() === '') {
                itemDataSibling.textContent = '(empty)';
                itemDataSibling.classList.add('empty-placeholder');
            }
        }
    });

    currentHighlightedIssueId = clickedIssueId;
}

function clearHighlights() {
    const highlightedElements = document.querySelectorAll('.item-data.highlighted');
    highlightedElements.forEach(element => {
        element.style.backgroundColor = '';
        element.classList.remove('highlighted');

        // Remove empty placeholder if it was added
        if (element.classList.contains('empty-placeholder')) {
            element.textContent = '';
            element.classList.remove('empty-placeholder');
        }
    });
}

// Clear highlights when clicking on the table (but not on issue squares)
document.getElementById('table-data').addEventListener('click', function(event) {
    if (!event.target.classList.contains('issue-icon')) {
        clearHighlights();
        currentHighlightedIssueId = null;
    }
});