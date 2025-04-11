<!-- PetNameGameView.vue -->
<template>
    <div>
      <!-- <h2 class="text-3xl font-bold mb-6">Pet Name Guessing Game</h2> -->
      <PetNameGame />
      
      <div class="mt-6">
        <router-link to="/" class="text-blue-500 hover:underline">
          ← Back to Games
        </router-link>
      </div>
    </div>
  </template>
  
  <script>
  import PetNameGame from '../../components/PetNameGame.vue'
  
  export default {
    name: 'PetNameGameView',
    components: {
      PetNameGame
    }
  }
  </script>