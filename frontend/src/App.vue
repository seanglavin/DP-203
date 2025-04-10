<template>
  <div class="app-container">
    <header class="bg-blue-600 text-white p-4">
      <h1 class="text-2xl font-bold">Vue Petfinder App</h1>
      <nav class="mt-2">
        <router-link to="/" class="mr-4 hover:underline">Home</router-link>
        <!-- <router-link to="/about" class="hover:underline">About</router-link> -->
      </nav>
    </header>
    
    <main class="container mx-auto p-4">
      <!-- This is where your views will be dynamically inserted -->
      <router-view></router-view>
    </main>
    
    <footer class="bg-gray-200 p-4 text-center">
      <p>© 2025 skglavin@gmail.com</p>
    </footer>
  </div>
</template>

<script>
export default {
  name: 'App'
}
</script>