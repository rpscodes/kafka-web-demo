let timer = null;
let flowInit = false;
let parts = [0, 1, 2]; // Initialize with expected partitions
let producedTotal = 0;
let consumedTotal = 0;
let knownIds = new Set();
let maxOffsetByPartition = {};

const KEY_POOL = ['k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7'];
const PALETTE = ['#3b82f6', '#f59e0b', '#10b981', '#8b5cf6', '#ef4444', '#06b6d4', '#84cc16', '#ec4899'];
const TOPIC = window.TOPIC || 'demo-topic';
let initialized = false;

// Load saved state from sessionStorage
try {
  const s = sessionStorage.getItem('maxSeen:' + TOPIC);
  if (s) maxOffsetByPartition = JSON.parse(s) || {};
} catch (_) {
  // Ignore parsing errors
}

function clearState() {
  try {
    sessionStorage.removeItem('maxSeen:' + TOPIC);
  } catch (_) {
    // Ignore storage errors
  }
  maxOffsetByPartition = {};
  initialized = false;
  producedTotal = 0;
  consumedTotal = 0;
  knownIds = new Set();
  lastData = [];
  keyToPartitionMap = {};
  
  // Clear table
  const tbody = document.querySelector('#tbl tbody');
  if (tbody) tbody.innerHTML = '';
  
  // Reset partition counts
  for (const partition of parts) {
    const partitionEl = document.querySelector(`[data-partition="${partition}"]`);
    if (partitionEl) {
      const countEl = partitionEl.querySelector('.partition-count');
      if (countEl) countEl.textContent = 'msgs: 0';
    }
  }
  
  flowInit = false;
}

let page = 1;
let pageSize = 20;
let lastData = [];


function hashIndex(str) {
  if (!str) return 0;
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = (h * 31 + str.charCodeAt(i)) | 0;
  }
  return Math.abs(h) % PALETTE.length;
}

function colorForKey(str) {
  return PALETTE[hashIndex(str)];
}

// Track actual key-to-partition mappings from sent messages
let keyToPartitionMap = {};

// Initialize with empty mappings - keys will only show after real messages are sent
function initializeKeyMappings() {
  // Start with empty mappings - only populate with real data from Kafka
  keyToPartitionMap = {};
}

// Function to get keys that belong to a specific partition based on actual data
function getKeysForPartition(partition) {
  return Object.keys(keyToPartitionMap).filter(key => keyToPartitionMap[key] === partition);
}

function initializeLegend() {
  const legend = document.getElementById('legend');
  if (!legend) return;
  
  legend.innerHTML = '';
  for (const k of KEY_POOL) { // Show all keys k0-k7
    const div = document.createElement('div');
    div.className = 'legend-item';
    const sw = document.createElement('span');
    sw.className = 'legend-dot';
    sw.style.background = colorForKey(k);
    const label = document.createElement('span');
    label.className = 'legend-label';
    label.textContent = k;
    div.appendChild(sw);
    div.appendChild(label);
    legend.appendChild(div);
  }
}

function ensurePartitionElements() {
  const partitionsContainer = document.getElementById('partitions');
  if (!partitionsContainer) return;
  
  // Remove existing partitions
  partitionsContainer.innerHTML = '';
  
  // Create partition elements for detected partitions
  for (const partition of parts) {
    const partitionDiv = document.createElement('div');
    partitionDiv.className = 'partition';
    partitionDiv.setAttribute('data-partition', partition);
    partitionDiv.innerHTML = `
      <div class="partition-label">Partition ${partition}</div>
      <div class="partition-keys" id="keys-${partition}"></div>
      <div class="partition-count">msgs: 0</div>
    `;
    partitionsContainer.appendChild(partitionDiv);
  }
  
  // Populate keys for each partition
  populatePartitionKeys();
}

function populatePartitionKeys() {
  // Only populate if partitions are initialized
  if (!parts || parts.length === 0) {
    console.log('populatePartitionKeys: parts not initialized yet');
    return;
  }
  
  console.log('populatePartitionKeys: parts =', parts);
  console.log('populatePartitionKeys: keyToPartitionMap =', keyToPartitionMap);
  
  for (const partition of parts) {
    const keysContainer = document.getElementById(`keys-${partition}`);
    if (!keysContainer) {
      console.log(`populatePartitionKeys: keys container not found for partition ${partition}`);
      continue;
    }
    
    const keysForPartition = getKeysForPartition(partition);
    console.log(`populatePartitionKeys: partition ${partition} keys =`, keysForPartition);
    
    keysContainer.innerHTML = '';
    
    keysForPartition.forEach(key => {
      const keyElement = document.createElement('span');
      keyElement.className = 'partition-key';
      keyElement.textContent = key;
      keyElement.style.backgroundColor = colorForKey(key);
      keysContainer.appendChild(keyElement);
    });
  }
}

// Conveyor belt animation - single message travels from producer to consumer
function animateMessageConveyor(key, partition) {
  console.log('Conveyor animation for key:', key, 'partition:', partition);
  const color = colorForKey(key);
  
  // Get all required elements
  const simulationArea = document.querySelector('.simulation-area');
  const producerCircle = document.querySelector('.producer-circle');
  const partitionEl = document.querySelector(`[data-partition="${partition}"]`);
  const consumerCircle = document.querySelector('.consumer-circle');
  
  console.log('Conveyor animation elements:', {
    simulationArea: !!simulationArea,
    producerCircle: !!producerCircle,
    partitionEl: !!partitionEl,
    consumerCircle: !!consumerCircle,
    partition: partition
  });
  
  if (!simulationArea || !producerCircle || !partitionEl || !consumerCircle) {
    console.error('Required elements not found for conveyor animation');
    return;
  }
  
  // Create animated message element
  const messageEl = document.createElement('div');
  messageEl.className = 'animated-message conveyor-message';
  messageEl.style.cssText = `
    position: absolute;
    width: 20px;
    height: 20px;
    background: ${color};
    border-radius: 50%;
    box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    z-index: 1000;
    pointer-events: none;
    opacity: 1;
    transform: scale(1);
    transition: none;
  `;
  
  simulationArea.appendChild(messageEl);
  console.log('Animated message element created and appended');
  
  // Get all positions
  const simulationRect = simulationArea.getBoundingClientRect();
  const producerRect = producerCircle.getBoundingClientRect();
  const partitionRect = partitionEl.getBoundingClientRect();
  const consumerRect = consumerCircle.getBoundingClientRect();
  
  console.log('Element positions:', {
    simulationRect,
    producerRect,
    partitionRect,
    consumerRect
  });
  
  // Calculate positions
  const startX = producerRect.left + producerRect.width / 2 - simulationRect.left;
  const startY = producerRect.top + producerRect.height / 2 - simulationRect.top;
  
  const partitionX = partitionRect.left + partitionRect.width / 2 - simulationRect.left;
  const partitionY = partitionRect.top + partitionRect.height / 2 - simulationRect.top;
  
  const endX = consumerRect.left + consumerRect.width / 2 - simulationRect.left;
  const endY = consumerRect.top + consumerRect.height / 2 - simulationRect.top;
  
  // Position at producer
  messageEl.style.left = startX + 'px';
  messageEl.style.top = startY + 'px';
  
  // Pulse producer circle
  producerCircle.classList.add('active');
  setTimeout(() => producerCircle.classList.remove('active'), 300);
  
  // Phase 1: Producer to Partition (1 second)
  setTimeout(() => {
    messageEl.style.transition = 'all 1s cubic-bezier(0.4, 0, 0.2, 1)';
    messageEl.style.left = partitionX + 'px';
    messageEl.style.top = partitionY + 'px';
    messageEl.style.transform = 'scale(0.9)';
    
    // Phase 2: Partition to Consumer (1 second) - starts after phase 1
    setTimeout(() => {
      messageEl.style.transition = 'all 1s cubic-bezier(0.4, 0, 0.2, 1)';
      messageEl.style.left = endX + 'px';
      messageEl.style.top = endY + 'px';
      messageEl.style.transform = 'scale(0.7)';
      messageEl.style.opacity = '0.8';
      
      // Pulse consumer circle
      consumerCircle.classList.add('active');
      setTimeout(() => consumerCircle.classList.remove('active'), 300);
      
      // Remove message after complete journey
      setTimeout(() => {
        if (messageEl.parentNode) {
          messageEl.parentNode.removeChild(messageEl);
        }
      }, 1000);
      
    }, 1000); // Start phase 2 after phase 1 completes
    
  }, 50); // Small delay to ensure positioning
}

// Note: animateMessageToConsumer function removed - now using single conveyor animation


function updateFlow(data) {
  // Initialize legend on first run
  if (!flowInit) {
    initializeLegend();
    // Detect actual partitions from data
    const actualPartitions = [...new Set(data.map(m => m.partition))].sort((a, b) => a - b);
    if (actualPartitions.length > 0) {
      parts = actualPartitions;
      console.log('Detected partitions:', parts);
      ensurePartitionElements();
    }
    flowInit = true;
  }
  
  if (!initialized) {
    const highest = {};
    for (const m of data) {
      const id = m.partition + ':' + m.offset;
      if (!knownIds.has(id)) knownIds.add(id);
      highest[m.partition] = Math.max(highest[m.partition] ?? -1, m.offset);
    }
    for (const p of parts) {
      maxOffsetByPartition[p] = highest[p] ?? -1;
    }
    try {
      sessionStorage.setItem('maxSeen:' + TOPIC, JSON.stringify(maxOffsetByPartition));
    } catch (_) {
      // Ignore storage errors
    }
    initialized = true;
    return;
  }
  
  let newSeen = 0;
  for (const m of data) {
    const id = m.partition + ':' + m.offset;
    if (!knownIds.has(id)) {
      knownIds.add(id);
      newSeen++;
    }
  }
  
  if (newSeen > 0) {
    consumedTotal += newSeen;
  }
}


async function bulk(n) {
  console.log('Bulk sending:', n, 'messages');
  const status = document.getElementById('status');
  if (!status) {
    console.error('Status element not found');
    return;
  }
  
  status.textContent = '📤 Sending ' + n + ' messages...';
  status.className = 'status';
  
  try {
    for (let i = 0; i < n; i++) {
      const key = KEY_POOL[i % KEY_POOL.length];
      const value = 'auto-' + Date.now() + '-' + i;
      const params = new URLSearchParams({ key, value });
      
      // Send message and get response
      const res = await fetch('/produce', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: params
      });
      
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }
      
      const txt = await res.text();
      console.log('Server response:', txt);
      
      // Extract partition from response
      const partitionMatch = txt.match(/partition (\d+)/);
      const partition = partitionMatch ? parseInt(partitionMatch[1]) : 0;
      console.log('Extracted partition:', partition);
      
      // Track the key-to-partition mapping
      keyToPartitionMap[key] = partition;
      
      // Trigger conveyor animation with delay
      setTimeout(() => {
        console.log('Conveyor animation for key:', key, 'partition:', partition);
        animateMessageConveyor(key, partition);
      }, i * 200); // Stagger conveyor animations
      
      await new Promise(r => setTimeout(r, 15));
    }
    
    producedTotal += n;
    status.textContent = '✅ Sent ' + n + ' messages successfully!';
    status.className = 'status';
    
    // Update partition keys display once after all messages are sent
    populatePartitionKeys();
    
    refresh();
  } catch (e) {
    console.error('Bulk send error:', e);
    status.textContent = '❌ Error: ' + e.message;
    status.className = 'status';
  }
}

function renderTable() {
  const tbody = document.querySelector('#tbl tbody');
  if (!tbody) {
    console.error('Table body not found');
    return;
  }
  
  tbody.innerHTML = '';
  const ps = pageSize;
  const start = (page - 1) * ps;
  const slice = lastData.slice(start, start + ps);
  
  for (const m of slice) {
    const tr = document.createElement('tr');
    const dt = new Date(m.ts).toLocaleString();
    tr.innerHTML = '<td>' + dt + '</td><td>' + m.partition + '</td><td>' + m.offset + '</td><td>' + (m.key || '') + '</td><td>' + (m.value || '') + '</td>';
    tbody.appendChild(tr);
  }
  
  const total = lastData.length;
  const pages = Math.max(1, Math.ceil(total / ps));
  if (page > pages) page = pages;
  
  const pageInfo = document.getElementById('pageInfo');
  const prevBtn = document.getElementById('prevBtn');
  const nextBtn = document.getElementById('nextBtn');
  
  if (pageInfo) pageInfo.textContent = 'Page ' + page + ' / ' + pages + ' · ' + total + ' rows';
  if (prevBtn) prevBtn.disabled = page <= 1;
  if (nextBtn) nextBtn.disabled = page >= pages;
}

async function refresh() {
  try {
    const res = await fetch('/messages');
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}: ${res.statusText}`);
    }
    
    const data = await res.json();
    if (!initialized) {
      updateFlow(data);
      updatePartitionCounts(data);
      lastData = [];
      renderTable();
  } else {
    // Check for new messages and update counts
    const newMessages = data.filter(m => {
      const id = m.partition + ':' + m.offset;
      return !knownIds.has(id);
    });
    
    // Note: Consumer animations are now handled by the conveyor belt animation
    // when messages are sent, not when they're consumed
    
    updateFlow(data);
    updatePartitionCounts(data);
    lastData = data;
    renderTable();
  }
  } catch (e) {
    console.error('Refresh error:', e);
    // Show error to user when server is not available
    const status = document.getElementById('status');
    if (status) {
      status.textContent = '❌ Error: Cannot connect to Kafka server';
      status.className = 'status';
    }
  }
}

async function resetAndRefresh() {
  try {
    const res = await fetch('/reset', { method: 'POST' });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}: ${res.statusText}`);
    }
    clearState();
    refresh();
  } catch (e) {
    console.error('Reset failed:', e);
    // Show error to user and prevent app from working
    const status = document.getElementById('status');
    if (status) {
      status.textContent = '❌ Error: Cannot connect to Kafka server';
      status.className = 'status';
    }
    throw e; // Re-throw to prevent app initialization
  }
}

// Event listeners will be set up in initializeApp()

// Update partition counts in the simulation area
function updatePartitionCounts(data) {
  const counts = {};
  for (const p of parts) {
    counts[p] = 0;
  }
  
  for (const m of data) {
    counts[m.partition]++;
  }
  
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i];
    const partitionEl = document.querySelector(`[data-partition="${p}"]`);
    if (partitionEl) {
      const countEl = partitionEl.querySelector('.partition-count');
      if (countEl) {
        countEl.textContent = `msgs: ${counts[p]}`;
      }
    }
  }
}

// Speed control functionality
let currentSpeed = 1.0;

// Global function for HTML onclick handlers
window.bulk = bulk;

// Test function to verify conveyor animations work
window.testAnimation = function() {
  console.log('Testing conveyor animation system...');
  console.log('Available partitions:', document.querySelectorAll('[data-partition]'));
  console.log('Simulation area:', document.querySelector('.simulation-area'));
  console.log('Producer circle:', document.querySelector('.producer-circle'));
  console.log('Consumer circle:', document.querySelector('.consumer-circle'));
  
  // Test conveyor animation to each partition
  parts.forEach((partition, index) => {
    setTimeout(() => {
      console.log(`Testing conveyor animation to partition ${partition}`);
      animateMessageConveyor(`test-key-${partition}`, partition);
    }, index * 1000); // Longer delay to see each animation clearly
  });
};

// Initialize when DOM is ready
function initializeApp() {
  console.log('DOM ready, initializing app...');
  
  // Set up event listeners
  const prevBtn = document.getElementById('prevBtn');
  const nextBtn = document.getElementById('nextBtn');
  const pageSizeSelect = document.getElementById('pageSize');
  const resetBtn = document.getElementById('resetBtn');
  const speedSlider = document.getElementById('speedSlider');
  
  if (prevBtn) {
    prevBtn.addEventListener('click', () => {
      if (page > 1) {
        page--;
        renderTable();
      }
    });
  }
  
  if (nextBtn) {
    nextBtn.addEventListener('click', () => {
      page++;
      renderTable();
    });
  }
  
  if (pageSizeSelect) {
    pageSizeSelect.addEventListener('change', e => {
      pageSize = parseInt(e.target.value, 10);
      page = 1;
      renderTable();
    });
  }
  
  if (resetBtn) {
    resetBtn.addEventListener('click', resetAndRefresh);
  }
  
  if (speedSlider) {
    speedSlider.addEventListener('input', function(e) {
      currentSpeed = parseFloat(e.target.value);
      const speedValue = document.getElementById('speedValue');
      if (speedValue) {
        speedValue.textContent = currentSpeed.toFixed(1) + 'x';
      }
      
      // Update the timer interval based on new speed
      if (timer) {
        clearInterval(timer);
        timer = setInterval(refresh, 1500 / currentSpeed);
      }
    });
  }
  
  
  // Initialize key mappings and populate keys for static partitions immediately
  initializeKeyMappings();
  populatePartitionKeys();
  
  // Reset and refresh - app should fail if Kafka is not available
  resetAndRefresh();
  
  timer = setInterval(refresh, 1500 / currentSpeed);
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initializeApp);
} else {
  // DOM is already ready
  initializeApp();
}
